#!/usr/bin/env nextflow

process SPLITLETTERS {
    
    input:
    tuple val(meta), val(input_str), val(out_name)

    output:
    tuple val(meta), path("${out_name}_*")

    script:
    """
    echo -n "${input_str}" |fold -w ${meta.block_size} | split -l 1 - ${out_name}_
    """
} 

process CONVERTTOUPPER {
    publishDir "results", mode: 'copy'

    input: 
    path chunck_file

    output:
    path "${chunck_file.simpleName}_upper.txt"

    script:
    """
    tr '[:lower:]' '[:upper:]' < "${chunck_file}" > "${chunck_file.simpleName}_upper.txt"
    cat "${chunck_file.simpleName}_upper.txt"
    """
} 

workflow { 
    // 1. Read in the samplesheet (samplesheet_2.csv)  into a channel. The block_size will be the meta-map
    in_ch = channel
        .fromPath('samplesheet_2.csv')
        .splitCsv(header: true)
        .map { row ->
            def meta = [
                block_size: row.block_size.toInteger()
            ]
            
            [meta, row.input_str, row.out_name]
        }
    // 2. Create a process that splits the "in_str" into sizes with size block_size. The output will be a file for each block, named with the prefix as seen in the samplesheet_2
    split_ch = SPLITLETTERS(in_ch)

    chuncks_ch = split_ch
        .map { meta, files -> files }
        .flatten()
    // 4. Feed these files into a process that converts the strings to uppercase. The resulting strings should be written to stdout

    CONVERTTOUPPER(chuncks_ch)

    // read in samplesheet}

    // split the input string into chunks

    // lets remove the metamap to make it easier for us, as we won't need it anymore

    // convert the chunks to uppercase and save the files to the results directory



}