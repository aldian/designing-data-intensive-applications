#include "simple_db_in_memory_index.h"
#include <cstdio> // For std::remove
#include <fstream>
#include <gtest/gtest.h>

class SimpleDbInMemoryIndexTest : public ::testing::Test {
protected:
  const std::string filename = "test.db";
  SimpleDbInMemoryIndex *db;

  void SetUp() override {
    db = new SimpleDbInMemoryIndex(filename);
    nlohmann::json greeting = {{"hello", "world"}};
    nlohmann::json menu = {{"breakfast", "bubur ayam"},
                           {"lunch", "nasi rendang"},
                           {"dinner", "nasi goreng"}};
    db->set("greeting", greeting);
    db->set("menu", menu);
  }

  void TearDown() override {
    delete db;
    std::remove(filename.c_str());
  }
};

// Test case for existing database
TEST_F(SimpleDbInMemoryIndexTest, ExistingDb) {
  SimpleDbInMemoryIndex db2(filename);
  auto idx_map =
      db2.get_index().get_idx_map(); // Assuming you have a method to get the
                                     // internal map for testing
  std::unordered_map<std::string, std::pair<long, size_t>> expected_idx_map = {
      {"greeting", {0, 26}}, {"menu", {26, 77}}};
  EXPECT_EQ(idx_map, expected_idx_map);
}

// Test case for setting values
TEST_F(SimpleDbInMemoryIndexTest, Set) {
  std::ifstream file(db->get_filename());
  ASSERT_TRUE(file.is_open());
  std::string line;
  std::getline(file, line);
  EXPECT_EQ(line, "greeting,{\"hello\":\"world\"}");
  std::getline(file, line);
  EXPECT_EQ(line, "menu,{\"breakfast\":\"bubur ayam\",\"dinner\":\"nasi "
                  "goreng\",\"lunch\":\"nasi rendang\"}");
  file.close();
}

// Test case for getting values
TEST_F(SimpleDbInMemoryIndexTest, Get) {
  nlohmann::json expected_menu = {{"breakfast", "bubur ayam"},
                                  {"lunch", "nasi rendang"},
                                  {"dinner", "nasi goreng"}};
  nlohmann::json expected_greeting = {{"hello", "world"}};
  EXPECT_EQ(db->get("menu"), expected_menu);
  EXPECT_EQ(db->get("greeting"), expected_greeting);
}

// Test case for getting invalid key
TEST_F(SimpleDbInMemoryIndexTest, GetInvalidKey) {
  EXPECT_EQ(db->get("invalid key"), nullptr);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}