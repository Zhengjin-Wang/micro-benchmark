//
// Created by lilac on 25-4-22.
//

#ifndef INSERT_HPP
#define INSERT_HPP
#include <bits/stdc++.h>
#include "../storage/segment.hpp"
#include "../storage/storage.hpp"
#include "../concurrency/transaction_manager.hpp"

// Ranges of rows to which the inserted values are written.
struct ChunkRange {
  ChunkID chunk_id{};
  ChunkOffset begin_chunk_offset{};
  ChunkOffset end_chunk_offset{};
};


void copy_value_range(const std::shared_ptr<const BaseSegment>& source_abstract_segment,
                      ChunkOffset source_begin_offset, const std::shared_ptr<BaseSegment>& target_abstract_segment,
                      ChunkOffset target_begin_offset, ChunkOffset length, const DataType& type) {
//  DebugAssert(source_abstract_segment->size() >= source_begin_offset + length, "Source Segment out-of-bounds.");
//  DebugAssert(target_abstract_segment->size() >= target_begin_offset + length, "Target Segment out-of-bounds.");
  if (std::holds_alternative<int>(type)) {
    auto target_value_segment = std::dynamic_pointer_cast<IntSegment>(target_abstract_segment);
    const auto source_value_segment = std::dynamic_pointer_cast<const IntSegment>(source_abstract_segment);

    auto& target_values = target_value_segment->values();
    std::copy_n(source_value_segment->values().begin() + source_begin_offset, length,
                  target_values.begin() + target_begin_offset);
  } else if (std::holds_alternative<float>(type)) {
    auto target_value_segment = std::dynamic_pointer_cast<FloatSegment>(target_abstract_segment);
    const auto source_value_segment = std::dynamic_pointer_cast<const FloatSegment>(source_abstract_segment);

    auto& target_values = target_value_segment->values();
    std::copy_n(source_value_segment->values().begin() + source_begin_offset, length,
                  target_values.begin() + target_begin_offset);
  } else if (std::holds_alternative<std::string>(type)) {
    auto target_value_segment = std::dynamic_pointer_cast<StringSegment>(target_abstract_segment);
    const auto source_value_segment = std::dynamic_pointer_cast<const StringSegment>(source_abstract_segment);

    auto& target_values = target_value_segment->values();
    std::copy_n(source_value_segment->values().begin() + source_begin_offset, length,
                  target_values.begin() + target_begin_offset);
  }

  /**
   * If the source Segment is a ValueSegment, take a fast path to copy the data. Otherwise, take a (potentially slower)
   * fallback path.
   */

}

std::shared_ptr<const Table> insert(std::shared_ptr<Table> _target_table, std::shared_ptr<const Table>& values_to_insert) {
  std::vector<ChunkRange> _target_chunk_ranges;

  // stage 1
  {
    const auto append_lock = _target_table->acquire_append_mutex();
    auto remaining_rows = values_to_insert->row_count();

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
      mvcc_data->register_insert();

      const auto num_rows_for_target_chunk =
          std::min<size_t>(_target_table->target_chunk_size() - target_chunk->size(), remaining_rows);

      _target_chunk_ranges.emplace_back(
          ChunkRange{target_chunk_id, target_chunk->size(),
                     static_cast<ChunkOffset>(target_chunk->size() + num_rows_for_target_chunk)});

      // Mark new (but still empty) rows as being under modification by current transaction. Do so before resizing the
      // Segments, because the resize of `Chunk::_segments.front()` is what releases the new row count.
      {
        const auto transaction_id = TransactionManager::get_next_transaction_id();
        const auto end_offset = target_chunk->size() + num_rows_for_target_chunk;
        for (auto target_chunk_offset = target_chunk->size(); target_chunk_offset < end_offset; ++target_chunk_offset) {
//          DebugAssert(mvcc_data->get_begin_cid(target_chunk_offset) == MvccData::MAX_COMMIT_ID, "Invalid begin CID.");
//          DebugAssert(mvcc_data->get_end_cid(target_chunk_offset) == MvccData::MAX_COMMIT_ID, "Invalid end CID.");
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
        auto segement = target_chunk->get_segment(column_id);
        segement->resize(new_size);

//		auto type = target_chunk->column_defs()[column_id].type;
//        if (std::holds_alternative<int>(type)) {
//          	auto segement = target_chunk->get_segment(column_id);
//        	return std::make_shared<IntSegment>(is_pk, range_lower_bound, range_upper_bound, num_rows);
//    	}
//    	else if (std::holds_alternative<float>(type)) {
//        	return std::make_shared<FloatSegment>(is_pk, range_lower_bound, range_upper_bound, num_rows);
//    	}
//    	else if (std::holds_alternative<std::string>(type)) {
//       	 	return std::make_shared<StringSegment>(is_pk, range_lower_bound, range_upper_bound, num_rows);
//    	}

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

  } // stage 1 end

    /**
   * 2. Insert the Data into the memory allocated in the first step without holding a lock on the Table.
   */
  auto source_row_id = RowID{ChunkID{0}, ChunkOffset{0}};

  for (const auto& target_chunk_range : _target_chunk_ranges) {
    const auto target_chunk = _target_table->get_chunk(target_chunk_range.chunk_id);

    auto target_chunk_offset = target_chunk_range.begin_chunk_offset;
    auto target_chunk_range_remaining_rows =
        ChunkOffset{target_chunk_range.end_chunk_offset - target_chunk_range.begin_chunk_offset};

    while (target_chunk_range_remaining_rows > 0) {
      const auto source_chunk = values_to_insert->get_chunk(source_row_id.chunk_id);
      const auto source_chunk_remaining_rows = ChunkOffset{source_chunk->size() - source_row_id.chunk_offset};
      const auto num_rows_current_iteration =
          std::min<ChunkOffset>(source_chunk_remaining_rows, target_chunk_range_remaining_rows);

      // Copy from the source into the target Segments.
      const auto column_count = target_chunk->column_count();
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        auto type = target_chunk->column_defs()[column_id].type;
        const auto& source_segment = source_chunk->get_segment(column_id);
        const auto& target_segment = target_chunk->get_segment(column_id);
        copy_value_range(source_segment, source_row_id.chunk_offset, target_segment,
                                           target_chunk_offset, num_rows_current_iteration, type);
      }

      if (num_rows_current_iteration == source_chunk_remaining_rows) {
        // Proceed to next source Chunk
        ++source_row_id.chunk_id;
        source_row_id.chunk_offset = 0;
      } else {
        source_row_id.chunk_offset += num_rows_current_iteration;
      }

      target_chunk_offset += num_rows_current_iteration;
      target_chunk_range_remaining_rows -= num_rows_current_iteration;
    }
  }

  return _target_table;
}

#endif //INSERT_HPP
