add_library(launcher STATIC
    launcher/launcher.cc
    launcher/thread_launcher.cc
    launcher/thread_pool_launcher.cc
    launcher/process_launcher.cc
    launcher/process_pool_launcher.cc
)
add_library(monitor STATIC monitor/perf_monitor.cc)
add_library(transaction STATIC txn/request.cc)
add_library(database STATIC db/database.cc)

# YOUR CODE HERE
# 
# Using CMake to create `db` executable, then link all the libraries above. 
#
# Find out the solution from the official documentation:
# https://cmake.org/cmake/help/latest/guide/tutorial/Adding%20a%20Library.html
#
# add_executable(db ...)
# target_link_libraries(...)
#
