CXX = g++
CXXFLAGS = -std=c++11 -Wall -fprofile-arcs -ftest-coverage
LDFLAGS = -lgcov --coverage -lgtest -lgtest_main

TARGET = test_simplest_database
SOURCES = simplest_database.cpp test_simplest_database.cpp
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET) *.gcda *.gcno

coverage: all run_tests generate_report

run_tests: $(TARGET)
	./$(TARGET)

generate_report:
	lcov --capture --directory . --output-file coverage.info --no-external --rc lcov_branch_coverage=1
	lcov --remove coverage.info '/usr/*' '*test_*' --output-file coverage.info
	genhtml coverage.info --output-directory out
	@echo "Coverage report generated in out/index.html"
