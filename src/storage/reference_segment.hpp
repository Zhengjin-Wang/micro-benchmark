//
// Created by lilac on 25-5-6.
//

#ifndef REFERENCE_SEGMENT_HPP
#define REFERENCE_SEGMENT_HPP
#include "segment.hpp"
#include "storage.hpp"

class ReferenceSegment : public BaseSegment {
public:
    using BaseSegment::BaseSegment;

    ReferenceSegment(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id,
                                   const std::vector<RowID>& pos)
    : _referenced_table(referenced_table),
      _referenced_column_id(referenced_column_id),
      _pos_list(pos){};

    void generate_random(size_t num_rows) override {
    }

    void resize(int32_t num_rows) override {
        _pos_list.resize(num_rows);
        _null_values.resize(num_rows);
    }

    const std::shared_ptr<const Table>& referenced_table() const {
        return _referenced_table;
    }

    ColumnID referenced_column_id() const {
        return _referenced_column_id;
    }

    const std::vector<RowID> pos_list() const {
      return _pos_list;
    }

    ChunkOffset size() const override {
        return _pos_list.size();
    }

    static std::shared_ptr<ReferenceSegment> create_reference_segment(std::shared_ptr<Table>& referenced_table, ColumnID referenced_column_id, ChunkID chunk_id);
    static std::shared_ptr<Table> create_reference_table(std::shared_ptr<Table>& referenced_table);

    std::vector<RowID> _pos_list;
    const std::shared_ptr<const Table> _referenced_table;
    const ColumnID _referenced_column_id;

};

std::shared_ptr<ReferenceSegment> ReferenceSegment::create_reference_segment(std::shared_ptr<Table>& referenced_table, ColumnID referenced_column_id, ChunkID chunk_id) {
    auto chunk = referenced_table->get_chunk(chunk_id);
    std::vector<RowID> pos_list(chunk->size());

    for(ChunkOffset i = 0; i < chunk->size(); ++i) {
      pos_list[i] = {chunk_id, i};
    }
    return std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, pos_list);
}

std::shared_ptr<Table> ReferenceSegment::create_reference_table(std::shared_ptr<Table>& referenced_table) {
    auto chunk_count = referenced_table->chunk_count();
    auto table = std::make_shared<Table>(referenced_table->column_defs());
    for(ChunkID i = 0; i < chunk_count; ++i) {
        auto chunk = referenced_table->get_chunk(i);
        std::vector<std::shared_ptr<BaseSegment>> segments;
        for(ColumnID j = 0; j < chunk->column_count(); ++j) {
          chunk->column_count();
          segments.emplace_back(ReferenceSegment::create_reference_segment(referenced_table, j, i));
        }
        auto reference_chunk =  std::make_shared<Chunk>(chunk->column_defs(), segments);
        table->append_reference_chunk(reference_chunk);
    }
    return table;
}

#endif //REFERENCE_SEGMENT_HPP
