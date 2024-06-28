#include "simplest_database.h"
#include <cstdio> // For std::remove
#include <errno.h>
#include <gtest/gtest.h>
#include <stdexcept> // For std::runtime_error
#include <sys/stat.h>
#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

class SimplestDatabaseTest : public ::testing::Test {
protected:
  const std::string filename = "test_database";
  SimplestDatabase *db;

  void SetUp() override { db = new SimplestDatabase(filename); }

  void TearDown() override {
    delete db;
    std::remove(filename.c_str());
  }

  void create_restricted_dir(const std::string &path) {
#ifdef _WIN32
    _mkdir(path.c_str());
    SetFileAttributes(path.c_str(), FILE_ATTRIBUTE_READONLY);
#else
    mkdir(path.c_str(), 0000);
#endif
  }

  void remove_restricted_dir(const std::string &path) {
#ifdef _WIN32
    SetFileAttributes(path.c_str(), FILE_ATTRIBUTE_NORMAL);
    _rmdir(path.c_str());
#else
    chmod(path.c_str(), 0700); // Restore permissions to allow deletion
    rmdir(path.c_str());
#endif
  }
};

// Test case for setting and getting values
TEST_F(SimplestDatabaseTest, SetAndGet) {
  try {
    // Set values
    db->set("key1", "value1");
    db->set("key2", "value2");

    // Get values
    EXPECT_EQ(db->get("key1"), "value1");
    EXPECT_EQ(db->get("key2"), "value2");
    EXPECT_EQ(db->get("non_existent_key"), "");
  } catch (const std::runtime_error &e) {
    FAIL() << "Exception thrown: " << e.what();
  }
}

// Test case for updating values
TEST_F(SimplestDatabaseTest, UpdateValue) {
  try {
    // Set values
    db->set("key1", "value1");
    db->set("key1", "value2");

    // Get updated value
    EXPECT_EQ(db->get("key1"), "value2");
  } catch (const std::runtime_error &e) {
    FAIL() << "Exception thrown: " << e.what();
  }
}

// Test case for file open error on set
TEST_F(SimplestDatabaseTest, SetFileOpenError) {
  const std::string restricted_dir = "restricted_dir";
  create_restricted_dir(restricted_dir);
  SimplestDatabase invalid_db(restricted_dir + "/test_database");
  try {
    invalid_db.set("key1", "value1");
    FAIL() << "Expected std::runtime_error";
  } catch (const std::runtime_error &e) {
    EXPECT_STREQ(e.what(), "Unable to open file for writing");
  } catch (...) {
    FAIL() << "Expected std::runtime_error";
  }
  remove_restricted_dir(restricted_dir);
}

// Test case for file open error on get
TEST_F(SimplestDatabaseTest, GetFileOpenError) {
  const std::string restricted_dir = "restricted_dir";
  create_restricted_dir(restricted_dir);
  SimplestDatabase invalid_db(restricted_dir + "/test_database");
  try {
    invalid_db.get("key1");
    FAIL() << "Expected std::runtime_error";
  } catch (const std::runtime_error &e) {
    EXPECT_STREQ(e.what(), "Unable to open file for reading");
  } catch (...) {
    FAIL() << "Expected std::runtime_error";
  }
  remove_restricted_dir(restricted_dir);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
