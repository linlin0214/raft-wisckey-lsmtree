#pragma once

#include "Config.h"
#include "SSTableReader.h"
#include "skiplist.h"
#include "Slice.h"
#include <string>
#include <vector>
#include <queue>
#include <memory>

class StorageIterator {
public:
    virtual ~StorageIterator() = default;
    virtual bool Valid() = 0;
    virtual void Next() = 0;
    virtual Slice Key() = 0;
    virtual std::string Value() = 0;
};

struct ImmContext;

class MemTableIteratorAdaptor : public StorageIterator {
public:
    explicit MemTableIteratorAdaptor(skiplist::Iterator it, std::shared_ptr<ImmContext> guard = nullptr)
        : it_(it), guard_(std::move(guard)) {}

    bool Valid() override { return it_.Valid(); }
    void Next() override { it_.Next(); }
    Slice Key() override { return it_.key(); }
    std::string Value() override { return it_.value(); }

private:
    skiplist::Iterator it_;
    std::shared_ptr<ImmContext> guard_;
};

class SSTableIteratorAdaptor : public StorageIterator {
public:
    explicit SSTableIteratorAdaptor(std::shared_ptr<SSTableReader> reader)
        : reader_(std::move(reader)), 
          it_(reader_ ? std::make_unique<SSTableIterator>(reader_.get()) : nullptr) {}

    bool Valid() override { return it_ && it_->Valid(); }
    void Next() override { if (it_) it_->Next(); }
    Slice Key() override { return it_->Key(); }
    std::string Value() override { return it_->Value(); }

private:
    std::shared_ptr<SSTableReader> reader_;
    std::unique_ptr<SSTableIterator> it_;
};

class MergingIterator : public StorageIterator {
private:
    struct HeapItem {
        std::string key;
        size_t priority;
        StorageIterator* iter;

        bool operator>(const HeapItem& other) const {
            if (key != other.key) {
                return Slice(key) > Slice(other.key);
            }
            return priority > other.priority;
        }
    };

public:
    explicit MergingIterator(std::vector<std::unique_ptr<StorageIterator>> children)
        : children_(std::move(children)), is_valid_(false) {
        for (size_t i = 0; i < children_.size(); ++i) {
            if (children_[i] && children_[i]->Valid()) {
                pq_.push(HeapItem{children_[i]->Key().ToString(), i, children_[i].get()});
            }
        }
        FindNext();
    }

    bool Valid() override { return is_valid_; }
    Slice Key() override { return Slice(current_key_); }
    std::string Value() override { return current_val_; }
    void Next() override { FindNext(); }

private:
    void FindNext() {
        while (!pq_.empty()) {
            HeapItem top = pq_.top();
            pq_.pop();

            std::string k = top.key;
            std::string v = top.iter->Value();

            top.iter->Next();
            if (top.iter->Valid()) {
                top.key = top.iter->Key().ToString();
                pq_.push(top);
            }

            if (has_last_key_ && Slice(k) == Slice(last_key_)) {
                continue;
            }

            last_key_ = k;
            has_last_key_ = true;

            if (v == config::TOMBSTONE) {
                continue;
            }

            current_key_ = std::move(k);
            current_val_ = std::move(v);
            is_valid_ = true;
            return;
        }
        is_valid_ = false;
    }

private:
    std::vector<std::unique_ptr<StorageIterator>> children_;
    std::priority_queue<HeapItem, std::vector<HeapItem>, std::greater<HeapItem>> pq_;
    bool is_valid_;
    std::string current_key_;
    std::string current_val_;
    bool has_last_key_{false};
    std::string last_key_;
};