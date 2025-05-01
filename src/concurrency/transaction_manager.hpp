//
// Created by lilac on 25-5-1.
//

#ifndef TRANSACTION_CONTEXT_H
#define TRANSACTION_CONTEXT_H

#include "../types.hpp"
TransactionID _transaction_id = INITIAL_TRANSACTION_ID;

namespace TransactionManager {

TransactionID get_next_transaction_id() {
    return _transaction_id++;
}

}
#endif //TRANSACTION_CONTEXT_H
