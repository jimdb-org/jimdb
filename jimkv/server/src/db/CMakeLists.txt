set(db_SOURCES
    db_manager.cpp

    mass_tree_impl/checkpoint.cpp
    mass_tree_impl/wal.cpp
    mass_tree_impl/mass_tree_wrapper.cpp

    mass_tree_impl/write_batch_impl.cpp
    mass_tree_impl/iterator_impl.cpp
    mass_tree_impl/db_impl.cpp
    mass_tree_impl/manager_impl.cpp

    rocksdb_impl/write_batch_impl.cpp
    rocksdb_impl/iterator_impl.cpp
    rocksdb_impl/db_impl.cpp
    rocksdb_impl/manager_impl.cpp
    )

foreach(f IN LISTS db_SOURCES)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"db/${f}\"")
endforeach()

add_library(jimkv-db STATIC ${db_SOURCES})
add_dependencies(jimkv-db build-3rd)