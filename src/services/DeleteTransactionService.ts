import { getCustomRepository } from 'typeorm';

import AppError from '../errors/AppError';
import TransactionRepository from '../repositories/TransactionsRepository';

class DeleteTransactionService {
  public async execute(id: string): Promise<void> {
    const transactionRepository = getCustomRepository(TransactionRepository);

    const findTransaction = await transactionRepository.findOne(id);

    if (!findTransaction) {
      throw new AppError('Transaction does not exist!');
    }

    await transactionRepository.remove(findTransaction);
  }
}

export default DeleteTransactionService;
