//
// Created by lilac on 25-4-22.
//

#ifndef INSERT_HPP
#define INSERT_HPP
#include <bits/stdc++.h>
#include "../storage/segment.hpp"
#include "../storage/storage.hpp"

std::shared_ptr<const Table> insert(std::shared_ptr<Table> _target_table, std::vector<int>& values_to_insert) {
  // stage 1
  {
    const auto append_lock = _target_table->acquire_append_mutex();
    auto remaining_rows = _target_table->row_count();

    if (_target_table->chunk_count() == 0) {
      _target_table->append_mutable_chunk();
    }

    while (remaining_rows > 0) {
      auto target_chunk_id = ChunkID{(ChunkID) _target_table->chunk_count() - 1};
      auto target_chunk = _target_table->get_chunk(target_chunk_id);
      const auto target_size = CHUNK_SIZE;

      // If the last chunk of the target table is either immutable or full, append a new mutable chunk.
      if (!target_chunk->is_mutable() || target_chunk->size() == target_size) {
        _target_table->append_mutable_chunk();
        ++target_chunk_id;
        target_chunk = _target_table->get_chunk(target_chunk_id);
      }

      // Register that Insert is pending. See `chunk.hpp`. for details.
      const auto& mvcc_data = target_chunk->mvcc_data();
      DebugAssert(mvcc_data, "Insert cannot operate on a table without MVCC data.");
      mvcc_data->register_insert();

      const auto num_rows_for_target_chunk =
          std::min<size_t>(_target_table->target_chunk_size() - target_chunk->size(), remaining_rows);

      _target_chunk_ranges.emplace_back(
          ChunkRange{target_chunk_id, target_chunk->size(),
                     static_cast<ChunkOffset>(target_chunk->size() + num_rows_for_target_chunk)});

      // Mark new (but still empty) rows as being under modification by current transaction. Do so before resizing the
      // Segments, because the resize of `Chunk::_segments.front()` is what releases the new row count.
      {
        const auto transaction_id = context->transaction_id();
        const auto end_offset = target_chunk->size() + num_rows_for_target_chunk;
        for (auto target_chunk_offset = target_chunk->size(); target_chunk_offset < end_offset; ++target_chunk_offset) {
          DebugAssert(mvcc_data->get_begin_cid(target_chunk_offset) == MvccData::MAX_COMMIT_ID, "Invalid begin CID.");
          DebugAssert(mvcc_data->get_end_cid(target_chunk_offset) == MvccData::MAX_COMMIT_ID, "Invalid end CID.");
          mvcc_data->set_tid(target_chunk_offset, transaction_id, std::memory_order_relaxed);
        }
      }

      // Make sure the MVCC data is written before the first segment (and, thus, the chunk) is resized.
      std::atomic_thread_fence(std::memory_order_seq_cst);

      // Grow data Segments.
      // Do so in REVERSE column order so that the resize of `Chunk::_segments.front()` happens last. It is this last
      // resize that makes the new row count visible to the outside world.
      const auto old_size = target_chunk->size();
      const auto new_size = old_size + num_rows_for_target_chunk;
      const auto column_count = target_chunk->column_count();
      for (auto reverse_column_id = ColumnID{0}; reverse_column_id < column_count; ++reverse_column_id) {
        const auto column_id = static_cast<ColumnID>(column_count - reverse_column_id - 1);

        resolve_data_type(_target_table->column_data_type(column_id), [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;

          const auto value_segment =
              std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(target_chunk->get_segment(column_id));
          Assert(value_segment, "Cannot insert into non-ValueSegments.");

          // Cannot guarantee resize without reallocation. The ValueSegment should have been allocated with the target
          // table's target chunk size reserved.
          Assert(value_segment->values().capacity() >= new_size, "ValueSegment too small.");
          value_segment->resize(new_size);
        });

        // Make sure the first column's resize actually happens last and does not get reordered.
        std::atomic_thread_fence(std::memory_order_seq_cst);
      }

      if (new_size == target_size) {
        // Allow the chunk to be marked as immutable as it has reached its target size and no incoming Insert operators
        // will try to write to it. Pending Insert operators (including us) will call `try_set_immutable()` to make the
        // chunk immutable once they commit/roll back.
        target_chunk->mark_as_full();
      }

      remaining_rows -= num_rows_for_target_chunk;
    }

  }
  return _target_table;
}

#endif //INSERT_HPP
