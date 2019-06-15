/**
 * lock_manager_test.cpp
 */

#include <thread>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {

// std::thread is movable
class scoped_guard {
  std::thread t;
public:
  explicit scoped_guard(std::thread t_) : t(std::move(t_)) {
    if (!t.joinable()) {
      throw std::logic_error("No thread");
    }
  }
  ~scoped_guard() {
    t.join();
  }
  scoped_guard(const scoped_guard &) = delete;
  scoped_guard &operator=(const scoped_guard &)= delete;
};

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
TEST(LockManagerTest, BasicTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
}

TEST(LockManagerTest, LockSharedTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  Transaction *txns[10];
  for (int i = 0; i < 10; i++) {
    txns[i] = txn_mgr.Begin();
    EXPECT_TRUE(lock_mgr.LockShared(txns[i], rid));
    EXPECT_EQ(TransactionState::GROWING, txns[i]->GetState());
  }
  for (auto &txn : txns) {
    txn_mgr.Commit(txn);
    EXPECT_EQ(TransactionState::COMMITTED, txn->GetState());
    delete txn;
  }
}

TEST(LockManagerTest, BasicExclusiveTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, p0, p1, p2;
  std::shared_future<void> ready(go.get_future());

  std::thread t0([&, ready] {
    Transaction txn(5);
    bool res = lock_mgr.LockExclusive(&txn, rid);

    p0.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t1([&, ready] {
    Transaction txn(3);

    p1.set_value();
    ready.wait();

    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

  });

  std::thread t2([&, ready] {
    Transaction txn(1);

    p2.set_value();
    ready.wait();

    // wait for t1
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

  });

  p0.get_future().wait();
  p1.get_future().wait();
  p2.get_future().wait();

  go.set_value();

  t0.join();
  t1.join();
  t2.join();
}

TEST(LockManagerTest, LockExclusiveTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  {
    std::mutex mu;
    Transaction txn1{1};
    EXPECT_TRUE(lock_mgr.LockShared(&txn1, rid));
    EXPECT_EQ(TransactionState::GROWING, txn1.GetState());

    std::thread t([&] {
      Transaction txn0{0};
      EXPECT_TRUE(lock_mgr.LockExclusive(&txn0, rid));
      EXPECT_EQ(TransactionState::GROWING, txn0.GetState());
      {
        std::lock_guard<std::mutex> lock{mu};
        EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
      }
      txn_mgr.Commit(&txn0);
      EXPECT_EQ(TransactionState::COMMITTED, txn0.GetState());
    });

    Transaction txn2{2};
    RID rid1{0, 1};
    EXPECT_TRUE(lock_mgr.LockExclusive(&txn2, rid1));
    EXPECT_EQ(TransactionState::GROWING, txn2.GetState());
    txn_mgr.Commit(&txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2.GetState());

    {
      std::lock_guard<std::mutex> lock{mu};
      txn_mgr.Commit(&txn1);
      EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
    }
    t.join();
  }

  {
    std::mutex mu;
    Transaction txn1{1};
    EXPECT_TRUE(lock_mgr.LockExclusive(&txn1, rid));
    EXPECT_EQ(TransactionState::GROWING, txn1.GetState());

    std::thread t([&] {
      Transaction txn0{0};
      EXPECT_TRUE(lock_mgr.LockShared(&txn0, rid));
      EXPECT_EQ(TransactionState::GROWING, txn0.GetState());
      {
        std::lock_guard<std::mutex> lock{mu};
        EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
      }
      txn_mgr.Commit(&txn1);
      EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
    });

    {
      std::lock_guard<std::mutex> lock{mu};
      txn_mgr.Commit(&txn1);
      EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
    }
    t.join();
  }
}

TEST(LockManagerTest, LockUpgradeTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  {
    Transaction txn{0};
    EXPECT_FALSE(lock_mgr.LockUpgrade(&txn, rid));
    EXPECT_EQ(TransactionState::ABORTED, txn.GetState());
    txn_mgr.Abort(&txn);
  }

  {
    Transaction txn{0};
    EXPECT_TRUE(lock_mgr.LockExclusive(&txn, rid));
    EXPECT_FALSE(lock_mgr.LockUpgrade(&txn, rid));
    EXPECT_EQ(TransactionState::ABORTED, txn.GetState());
    txn_mgr.Abort(&txn);
  }

  {
    Transaction txn{0};
    EXPECT_TRUE(lock_mgr.LockShared(&txn, rid));
    EXPECT_TRUE(lock_mgr.LockUpgrade(&txn, rid));
    txn_mgr.Commit(&txn);
  }

  {
    std::mutex mu;
    Transaction txn0{0};
    Transaction txn1{1};
    EXPECT_TRUE(lock_mgr.LockShared(&txn1, rid));

    std::thread t([&] {
      EXPECT_TRUE(lock_mgr.LockShared(&txn0, rid));
      EXPECT_TRUE(lock_mgr.LockUpgrade(&txn0, rid));
      {
        std::lock_guard<std::mutex> lock{mu};
        EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
      }
      txn_mgr.Commit(&txn0);
      EXPECT_EQ(TransactionState::COMMITTED, txn0.GetState());
    });

    {
      std::lock_guard<std::mutex> lock{mu};
      txn_mgr.Commit(&txn1);
      EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
    }
    t.join();
  }
}

TEST(LockManagerTest, BasicUpdateTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, p0, p1, p2, p3;
  std::shared_future<void> ready(go.get_future());

  std::thread t0([&, ready] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);

    p0.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // update
    res = lock_mgr.LockUpgrade(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t1([&, ready] {
    Transaction txn(1);

    bool res = lock_mgr.LockShared(&txn, rid);

    p1.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t2([&, ready] {
    Transaction txn(2);
    bool res = lock_mgr.LockShared(&txn, rid);

    p2.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t3([&, ready] {
    Transaction txn(3);
    bool res = lock_mgr.LockShared(&txn, rid);

    p3.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  p0.get_future().wait();
  p1.get_future().wait();
  p2.get_future().wait();
  p3.get_future().wait();

  go.set_value();

  t0.join();
  t1.join();
  t2.join();
  t3.join();
}

TEST(LockManagerTest, 2plTest) {
  LockManager lock_mgr{false};
  Transaction txn{0};
  RID rid0{0}, rid1{1};

  EXPECT_TRUE(lock_mgr.LockShared(&txn, rid0));
  EXPECT_TRUE(lock_mgr.Unlock(&txn, rid0));
  EXPECT_EQ(TransactionState::SHRINKING, txn.GetState());
  EXPECT_FALSE(lock_mgr.LockShared(&txn, rid1));
  EXPECT_EQ(TransactionState::ABORTED, txn.GetState());
}

TEST(LockManagerTest, S2plTest) {
  LockManager lock_mgr{true};
  RID rid{0};

  {
    Transaction txn{0};
    EXPECT_TRUE(lock_mgr.LockShared(&txn, rid));
    EXPECT_FALSE(lock_mgr.Unlock(&txn, rid));
    EXPECT_EQ(TransactionState::ABORTED, txn.GetState());
  }

  {
    Transaction txn{0};
    EXPECT_TRUE(lock_mgr.LockShared(&txn, rid));
    txn.SetState(TransactionState::COMMITTED);
    EXPECT_TRUE(lock_mgr.Unlock(&txn, rid));
  }
}


TEST(LockManagerTest, BasicTest1) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, t0, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(2);

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    t0.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    t1.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread2([&, ready] {
    Transaction txn(0);

    t2.set_value();
    ready.wait();

    // can wait and will block
    bool res = lock_mgr.LockExclusive(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  t0.get_future().wait();
  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
  thread2.join();
}

TEST(LockManagerTest, BasicTest2) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, t0, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(0);

    t0.set_value();
    ready.wait();

    // let thread1 try to acquire shared lock first
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    t1.set_value();
    ready.wait();

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);


    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread2([&, ready] {
    Transaction txn(2);

    // can wait and will block
    bool res = lock_mgr.LockExclusive(&txn, rid);

    t2.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  t0.get_future().wait();
  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
  thread2.join();
}

// basic wait-die test
//TEST(LockManagerTest, WaitDieTest) {
//  LockManager lock_mgr{false};
//  TransactionManager txn_mgr{&lock_mgr};
//  RID rid{0};
//
//  std::promise<void> done;
//  auto f = done.get_future();
//  Transaction txn0{0}, txn1{1}, txn2{2}, txn3{3};
//  lock_mgr.LockShared(&txn2, rid);
//  EXPECT_FALSE(lock_mgr.LockExclusive(&txn3, rid));
//
//  std::thread t0([&] {
//    done.set_value();
//    EXPECT_TRUE(lock_mgr.LockExclusive(&txn0, rid));
//    lock_mgr.Unlock(&txn0, rid);
//  });
//  f.get();
//  std::this_thread::yield();
//  // unstable
//  EXPECT_FALSE(lock_mgr.LockExclusive(&txn1, rid));
//  EXPECT_TRUE(lock_mgr.Unlock(&txn2, rid));
//  t0.join();
//}
//TEST(LockManagerTest, DeadlockTest1) {
//  LockManager lock_mgr{false};
//  TransactionManager txn_mgr{&lock_mgr};
//  RID rid{0, 0};
//
//  std::promise<void> go, go2, t1, t2;
//  std::shared_future<void> ready(go.get_future());
//
//  std::thread thread0([&, ready] {
//    Transaction txn(0);
//    bool res = lock_mgr.LockShared(&txn, rid);
//
//    t1.set_value();
//    ready.wait();
//
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    // waiting thread2 call LockExclusive before unlock
//    go2.get_future().wait();
//
//    // unlock
//    res = lock_mgr.Unlock(&txn, rid);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//  });
//
//  std::thread thread1([&, ready] {
//    Transaction txn(1);
//
//    // wait thread t0 to get shared lock first
//    t2.set_value();
//    ready.wait();
//
//    bool res = lock_mgr.LockExclusive(&txn, rid);
//    go2.set_value();
//
//    EXPECT_EQ(res, false);
//    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
//  });
//
//  t1.get_future().wait();
//  t2.get_future().wait();
//
//  // go!
//  go.set_value();
//
//  thread0.join();
//  thread1.join();
//}
//
//TEST(LockManagerTest, DeadlockTest2) {
//  LockManager lock_mgr{false};
//  TransactionManager txn_mgr{&lock_mgr};
//  RID rid{0, 0};
//
//  std::promise<void> go, t1, t2;
//  std::shared_future<void> ready(go.get_future());
//
//  std::thread thread0([&, ready] {
//    Transaction txn(0);
//
//    t1.set_value();
//    ready.wait();
//
//    // will block and can wait
//    bool res = lock_mgr.LockShared(&txn, rid);
//
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    // unlock
//    res = lock_mgr.Unlock(&txn, rid);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//  });
//
//  std::thread thread1([&, ready] {
//    Transaction txn(1);
//
//    bool res = lock_mgr.LockExclusive(&txn, rid);
//
//    t2.set_value();
//    ready.wait();
//
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    res = lock_mgr.Unlock(&txn, rid);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//  });
//
//  t1.get_future().wait();
//  t2.get_future().wait();
//
//  // go!
//  go.set_value();
//
//  thread0.join();
//  thread1.join();
//}
//
//TEST(LockManagerTest, DeadlockTest3) {
//  LockManager lock_mgr{false};
//  TransactionManager txn_mgr{&lock_mgr};
//  RID rid{0, 0};
//  RID rid2{0, 1};
//
//  std::promise<void> go, t1, t2;
//  std::shared_future<void> ready(go.get_future());
//
//  std::thread thread0([&, ready] {
//    Transaction txn(0);
//
//    // try get exclusive lock on rid2, will succeed
//    bool res = lock_mgr.LockExclusive(&txn, rid2);
//
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    t1.set_value();
//    ready.wait();
//
//    // will block and can wait
//    res = lock_mgr.LockShared(&txn, rid);
//
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    // unlock rid1
//    res = lock_mgr.Unlock(&txn, rid);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//
//    // unblock rid2
//    res = lock_mgr.Unlock(&txn, rid2);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//  });
//
//  std::thread thread1([&, ready] {
//    Transaction txn(1);
//
//    // try to get shared lock on rid, will succeed
//    bool res = lock_mgr.LockExclusive(&txn, rid);
//
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    t2.set_value();
//    ready.wait();
//
//    res = lock_mgr.LockShared(&txn, rid2);
//    EXPECT_EQ(res, false);
//    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
//
//    std::this_thread::sleep_for(std::chrono::milliseconds(500));
//
//    // unlock rid
//    res = lock_mgr.Unlock(&txn, rid);
//
//    EXPECT_EQ(res, true);
//
//    if (txn.GetState() == TransactionState::GROWING) {
//      LOG_INFO("result of young lock state is growing");
//    }
//    if (txn.GetState() == TransactionState::ABORTED) {
//      LOG_INFO("result of young lock state is ABORTED");
//    }
//    if (txn.GetState() == TransactionState::SHRINKING) {
//      LOG_INFO("result of young lock state is SHRINKING");
//    }
//    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
//  });
//
//  t1.get_future().wait();
//  t2.get_future().wait();
//
//  // go!
//  go.set_value();
//
//  thread0.join();
//  thread1.join();
//}

} // namespace cmudb
