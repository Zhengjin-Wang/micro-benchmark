//
// Created by zhengjin.wang on 25-4-29.
//

#ifndef MVCC_DATA_H
#define MVCC_DATA_H

#include <iosfwd>
#include <vector>

#include "../utils/copyable_atomic.hpp"
#include "../types.hpp"

struct MvccData {
  friend class Chunk;
  friend std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data);

 public:
  // The last commit id is reserved for uncommitted changes
  static constexpr CommitID MAX_COMMIT_ID = CommitID{UINT_MAX - 1};

  // This is used for optimizing the validation process. It is set during `Chunk::set_immutable()` and for each
  // commit of an Insert/Delete operator. Consult `Validate::_on_execute()` for further details.
  std::atomic<CommitID> max_begin_cid{MAX_COMMIT_ID};
  std::atomic<CommitID> max_end_cid{MAX_COMMIT_ID};

  // Creates MVCC data that supports a maximum of `size` rows. If the underlying chunk has less rows, the extra rows
  // here are ignored. This is to avoid resizing the vectors, which would cause reallocations and require locking.
  explicit MvccData(const size_t size, CommitID begin_commit_id);

  /**
   * The thread sanitizer (tsan) complains about concurrent writes and reads to begin/end_cids. That is because it is
   * unaware of their thread-safety being guaranteed by the update of the global last_cid. Furthermore, we exploit that
   * writes up to eight bytes are atomic on x64, which C++ and tsan do not know about. These helper methods were added
   * to .tsan-ignore.txt and can be used (carefully) to avoid those false positives.
   */
  CommitID get_begin_cid(const ChunkOffset offset) const;
  void set_begin_cid(const ChunkOffset offset, const CommitID commit_id);

  CommitID get_end_cid(const ChunkOffset offset) const;
  void set_end_cid(const ChunkOffset offset, const CommitID commit_id);

  TransactionID get_tid(const ChunkOffset offset) const;
  void set_tid(const ChunkOffset offset, const TransactionID transaction_id,
               const std::memory_order memory_order = std::memory_order_seq_cst);
  bool compare_exchange_tid(const ChunkOffset offset, TransactionID expected_transaction_id,
                            TransactionID new_transaction_id);

  size_t memory_usage() const;

  // Register and deregister Insert operators that write to the chunk. We use this information to notice when all
  // Inserts are either committed or rolled back and if we can mark a chunk as immutable. For more details, see
  // `chunk.hpp`.
  void register_insert();
  void deregister_insert();
  uint32_t pending_inserts() const;

 private:
  // These vectors are pre-allocated. Do not resize them as someone might be reading them concurrently.
  std::vector<CommitID> _begin_cids;                  // < CommitID when record was added
  std::vector<CommitID> _end_cids;                    // < CommitID when record was deleted
  std::vector<copyable_atomic<TransactionID>> _tids;  // < 0 unless locked by a transaction

  std::atomic_uint32_t _pending_inserts{0};
};

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data);


#endif //MVCC_DATA_H
