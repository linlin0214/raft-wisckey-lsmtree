#pragma once

#include <atomic>
#include <string>
#include <sstream>
#include <vector>
#include <array>
#include <algorithm>
#include <cstdint>
#include <cmath>

class Metrics {
public:
    static Metrics& Instance() {
        static Metrics instance;
        return instance;
    }

    // --- Counter & Gauge 操作 ---
    void IncCommittedOps() { committed_ops_.fetch_add(1, std::memory_order_relaxed); }
    void IncRejectedOps()  { rejected_ops_.fetch_add(1, std::memory_order_relaxed); }
    void IncReadOps()      { read_ops_.fetch_add(1, std::memory_order_relaxed); }
    
    void IncActiveConnections() { active_connections_.fetch_add(1, std::memory_order_relaxed); }
    void DecActiveConnections() { active_connections_.fetch_sub(1, std::memory_order_relaxed); }

    // --- 直方图与环形时延采样 (单位: 微秒 us) ---
    void ObserveCommitLatency(uint64_t lat_us) {
        // 1. 累加总耗时与总观测次数
        latency_sum_us_.fetch_add(lat_us, std::memory_order_relaxed);
        latency_count_.fetch_add(1, std::memory_order_relaxed);

        // 2. 累加 Prometheus 标准 Bucket
        for (size_t i = 0; i < kBucketUpperBoundsUs.size(); ++i) {
            if (lat_us <= kBucketUpperBoundsUs[i]) {
                bucket_counts_[i].fetch_add(1, std::memory_order_relaxed);
            }
        }

        // 3. 压入环形无锁采样池 (供动态精准计算 P50/P90/P99)
        size_t idx = sample_write_idx_.fetch_add(1, std::memory_order_relaxed) & (kReservoirSize - 1);
        latency_reservoir_[idx].store(static_cast<uint32_t>(std::min<uint64_t>(lat_us, UINT32_MAX)), std::memory_order_relaxed);
    }

    // 格式化输出 OpenMetrics / Prometheus 文本标准
    template <typename StatusType>
    std::string RenderPrometheus(const StatusType& status) {
        std::stringstream ss;

        // 1. 节点基础拓扑与 Raft 水位元数据 (Gauge)
        ss << "# HELP raft_node_info Node information and status\n";
        ss << "# TYPE raft_node_info gauge\n";
        ss << "raft_node_info{node_id=\"" << status.node_id 
           << "\",role=\"" << status.role 
           << "\",leader_id=\"" << (status.leader_id == UINT32_MAX ? 0 : status.leader_id) 
           << "\"} 1\n\n";

        ss << "# HELP raft_term Current Raft Term\n";
        ss << "# TYPE raft_term gauge\n";
        ss << "raft_term " << status.term << "\n\n";

        ss << "# HELP raft_last_log_index Last appended log index\n";
        ss << "# TYPE raft_last_log_index gauge\n";
        ss << "raft_last_log_index " << status.last_log_index << "\n\n";

        ss << "# HELP raft_commit_index Commit index\n";
        ss << "# TYPE raft_commit_index gauge\n";
        ss << "raft_commit_index " << status.commit_index << "\n\n";

        ss << "# HELP raft_applied_index State machine applied index\n";
        ss << "# TYPE raft_applied_index gauge\n";
        ss << "raft_applied_index " << status.last_applied << "\n\n";

        // 2. 网络活跃连接数 (Gauge)
        ss << "# HELP raft_active_connections Current active client TCP connections\n";
        ss << "# TYPE raft_active_connections gauge\n";
        ss << "raft_active_connections " << active_connections_.load(std::memory_order_relaxed) << "\n\n";

        // 3. 业务吞吐分类计数器 (Counter)
        ss << "# HELP raft_operations_total Total client requests processed\n";
        ss << "# TYPE raft_operations_total counter\n";
        ss << "raft_operations_total{type=\"put\",status=\"committed\"} " << committed_ops_.load(std::memory_order_relaxed) << "\n";
        ss << "raft_operations_total{type=\"put\",status=\"rejected\"} " << rejected_ops_.load(std::memory_order_relaxed) << "\n";
        ss << "raft_operations_total{type=\"get\",status=\"success\"} " << read_ops_.load(std::memory_order_relaxed) << "\n\n";

        // 4. 标准 Prometheus 延迟直方图 (Histogram)
        ss << "# HELP raft_commit_latency_microseconds Commit latency distribution in microseconds\n";
        ss << "# TYPE raft_commit_latency_microseconds histogram\n";
        
        uint64_t cumulative = 0;
        for (size_t i = 0; i < kBucketUpperBoundsUs.size(); ++i) {
            cumulative += bucket_counts_[i].load(std::memory_order_relaxed);
            ss << "raft_commit_latency_microseconds_bucket{le=\"" << (kBucketUpperBoundsUs[i] / 1000.0) << "ms\"} " 
               << cumulative << "\n";
        }
        ss << "raft_commit_latency_microseconds_bucket{le=\"+Inf\"} " 
           << latency_count_.load(std::memory_order_relaxed) << "\n";
        ss << "raft_commit_latency_microseconds_sum " 
           << latency_sum_us_.load(std::memory_order_relaxed) << "\n";
        ss << "raft_commit_latency_microseconds_count " 
           << latency_count_.load(std::memory_order_relaxed) << "\n\n";

        // 5. 动态采样计算百分位指标 (P50, P90, P99, Max)
        auto [p50, p90, p99, max_lat] = ComputeQuantiles();
        ss << "# HELP raft_commit_latency_quantiles_microseconds Dynamic sampled percentile latency in microseconds\n";
        ss << "# TYPE raft_commit_latency_quantiles_microseconds gauge\n";
        ss << "raft_commit_latency_quantiles_microseconds{quantile=\"0.50\"} " << p50 << "\n";
        ss << "raft_commit_latency_quantiles_microseconds{quantile=\"0.90\"} " << p90 << "\n";
        ss << "raft_commit_latency_quantiles_microseconds{quantile=\"0.99\"} " << p99 << "\n";
        ss << "raft_commit_latency_quantiles_microseconds{quantile=\"max\"} "  << max_lat << "\n";

        return ss.str();
    }

private:
    Metrics() {
        for (auto& count : bucket_counts_) {
            count.store(0, std::memory_order_relaxed);
        }
        for (auto& item : latency_reservoir_) {
            item.store(0, std::memory_order_relaxed);
        }
    }

    struct Quantiles {
        double p50{0.0};
        double p90{0.0};
        double p99{0.0};
        double max_val{0.0};
    };

    Quantiles ComputeQuantiles() {
        size_t total_written = sample_write_idx_.load(std::memory_order_relaxed);
        if (total_written == 0) return {};

        size_t sample_count = std::min<size_t>(total_written, kReservoirSize);
        std::vector<uint32_t> samples;
        samples.reserve(sample_count);

        for (size_t i = 0; i < sample_count; ++i) {
            uint32_t val = latency_reservoir_[i].load(std::memory_order_relaxed);
            if (val > 0) samples.push_back(val);
        }

        if (samples.empty()) return {};

        std::sort(samples.begin(), samples.end());
        size_t n = samples.size();

        return Quantiles{
            static_cast<double>(samples[static_cast<size_t>(n * 0.50)]),
            static_cast<double>(samples[static_cast<size_t>(n * 0.90)]),
            static_cast<double>(samples[static_cast<size_t>(n * 0.99)]),
            static_cast<double>(samples.back())
        };
    }

private:
    std::atomic<int64_t> active_connections_{0};
    std::atomic<uint64_t> committed_ops_{0};
    std::atomic<uint64_t> rejected_ops_{0};
    std::atomic<uint64_t> read_ops_{0};

    std::atomic<uint64_t> latency_sum_us_{0};
    std::atomic<uint64_t> latency_count_{0};

    // 标准延迟桶: 0.5ms, 1ms, 2ms, 5ms, 10ms, 20ms, 50ms, 100ms, 250ms, 500ms
    inline static const std::vector<uint64_t> kBucketUpperBoundsUs = {
        500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 250000, 500000
    };
    std::array<std::atomic<uint64_t>, 10> bucket_counts_;

    // 环形无锁采样池 (大小 16384，占用仅 64KB 内存)
    static constexpr size_t kReservoirSize = 16384;
    std::atomic<size_t> sample_write_idx_{0};
    std::array<std::atomic<uint32_t>, kReservoirSize> latency_reservoir_;
};