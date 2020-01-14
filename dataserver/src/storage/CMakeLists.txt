set(storage_SOURCES
    aggregate_calc.cpp
    field_value.cpp
    kv_fetcher.cpp
    meta_store.cpp
    metric.cpp
    row_decoder.cpp
    row_fetcher.cpp
    snapshot.cpp
    store.cpp
    store_txn.cpp
    util.cpp
    processor.cpp
    processor_table_read.cpp
    processor_index_read.cpp
    processor_limit.cpp
    processor_selection.cpp
    processor_agg.cpp
    processor_stream_agg.cpp
    processor_order_by.cpp
    processor_data_sample.cpp
    internal_tag.pb.cc
    stats_idx.cpp
    stats_col.cpp
    histogram.cpp
    )

foreach(f IN LISTS storage_SOURCES)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"storage/${f}\"")
endforeach()

add_library(jim-storage STATIC ${storage_SOURCES})
add_dependencies(jim-storage jim-proto)
