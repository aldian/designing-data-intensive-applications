CXX = g++
CXXFLAGS = -std=c++11 -Wall -fprofile-arcs -ftest-coverage
LDFLAGS = -lgcov --coverage -lgtest -lgtest_main

TARGET = test_simple_db_in_memory_index
SOURCES = simple_db_in_memory_index.cpp test_simple_db_in_memory_index.cpp
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

%.o: %.cpp %.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET) *.gcda *.gcno coverage.info
	rm -rf out

coverage: all run_tests generate_report

run_tests: $(TARGET)
	./$(TARGET)

generate_report:
	lcov --capture --directory . --output-file coverage.info --no-external --rc lcov_branch_coverage=1
	lcov --remove coverage.info '/usr/*' '*test_*' --output-file coverage.info
	genhtml coverage.info --output-directory out
	@echo "Coverage report generated in out/index.html"

format:
	clang-format -i *.cpp *.h