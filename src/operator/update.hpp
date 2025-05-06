//
// Created by lilac on 25-5-6.
//

#ifndef UPDATE_HPP
#define UPDATE_HPP
#include <bits/stdc++.h>
#include "../storage/segment.hpp"
#include "../storage/storage.hpp"
#include "../concurrency/transaction_manager.hpp"
#include "delete.hpp"
#include "insert.hpp"

std::shared_ptr<const Table> update(std::shared_ptr<Table> _target_table, std::shared_ptr<const Table>& values_to_insert, std::shared_ptr<Table>& rows_to_delete){
      delete_rows(rows_to_delete);
      insert(_target_table, values_to_insert);
      return nullptr;
}

#endif //UPDATE_HPP
