//
// Created by lilac on 25-5-6.
//

#ifndef DELETE_H
#define DELETE_H
#include "../storage/storage.hpp"
#include "../concurrency/transaction_manager.hpp"
#include "../storage/reference_segment.hpp"

std::shared_ptr<const Table> delete_rows(std::shared_ptr<Table>& _referencing_table) {
   auto _transaction_id = TransactionManager::get_next_transaction_id();

  const auto chunk_count = _referencing_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = _referencing_table->get_chunk(chunk_id);

//    Assert(chunk->references_exactly_one_table(), "All segments in _referencing_table must reference the same table.");

    const auto first_segment = std::static_pointer_cast<const ReferenceSegment>(chunk->get_segment(ColumnID{0}));
    const auto pos_list = first_segment->pos_list();

    for (const auto row_id : pos_list) {
      const auto referenced_chunk = first_segment->referenced_table()->get_chunk(row_id.chunk_id);
      Assert(referenced_chunk, "Referenced chunks are not allowed to be null pointers.");

      const auto& mvcc_data = referenced_chunk->mvcc_data();
//      DebugAssert(mvcc_data, "Delete cannot operate on a table without MVCC data.");

      // We validate every row again, which ensures we notice if we try to delete the same row twice within a single
      // statement. This could happen if the input table references the same row multiple times, e.g., due to a UnionAll
      // operator. Though marking a row as deleted multiple times from the same statement/transaction is no problem,
      // multiple deletions currently lead to an incorrect invalid_row_count of the chunk containing the affected row.
//      DebugAssert(
//          Validate::is_row_visible(
//              context->transaction_id(), context->snapshot_commit_id(), mvcc_data->get_tid(row_id.chunk_offset),
//              mvcc_data->get_begin_cid(row_id.chunk_offset), mvcc_data->get_end_cid(row_id.chunk_offset)),
//          "Trying to delete a row that is not visible to the current transaction. Has the input been validated?");

      // Actual row "lock" for delete happens here, making sure that no other transaction can delete this row.
      const auto expected = TransactionID{0};
      const auto success = mvcc_data->compare_exchange_tid(row_id.chunk_offset, expected, _transaction_id);

      if (!success) {
        // If the row has a set TID, it might be a row that our transaction inserted. No need to compare-and-swap here,
        // because we can only run into conflicts when two transactions try to change this row from the initial TID.
        if (mvcc_data->get_tid(row_id.chunk_offset) == _transaction_id) {
          // Make sure that even we do not see the row anymore.
          mvcc_data->set_tid(row_id.chunk_offset, INVALID_TRANSACTION_ID);
        } else {
          // The row is already locked by someone else and the transaction needs to be rolled back.
//          _mark_as_failed();
          return nullptr;
        }
      }
    }
  }
    return nullptr;
}
#endif //DELETE_H
