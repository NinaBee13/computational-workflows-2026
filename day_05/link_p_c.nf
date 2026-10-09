#!/usr/bin/env nextflow

process SPLITLETTERS {
input:
tuple val(meta), val(data)      

output:
tuple val(meta), path("${data[1]}_*")

script:
def in_str   = data[0]
def out_name = data[1]
def block_size = meta.metadata_1
"""
#!/home/nina/miniconda3/envs/ComputWorkflows/bin/python

in_str = ${in_str.inspect()}
block_size = ${block_size}
out_name = ${out_name.inspect()}

for i in range(0, len(in_str), block_size):
    chunk = in_str[i:i + block_size]
    filename = f"{out_name}_{i // block_size + 1}.txt"

    with open(filename, "w") as f:
        f.write(chunk)
"""
}

process CONVERTTOUPPER {
publishDir '/home/nina/computational-workflows-2026/day_05/results'

input:
path chunk

output:
path "upper_*"

script:
"""
cat ${chunk} | tr '[a-z]' '[A-Z]' > upper_${chunk.name}
"""
} 

workflow { 
    // 1. Read in the samplesheet (samplesheet_2.csv)  into a channel. The block_size will be the meta-map
       in_ch = channel.fromPath('samplesheet_2.csv').splitCsv(header:true).map{row ->
    [[metadata_1: row.block_size],[ row.input_str, row.out_name]]}
    in_ch.view()
    
    // 2. Create a process that splits the "in_str" into sizes with size block_size. The output will be a file for each block, named with the prefix as seen in the samplesheet_2
        split_ch = SPLITLETTERS(in_ch)
    // 4. Feed these files into a process that converts the strings to uppercase. The resulting strings should be written to stdout

    // read in samplesheet}

    // split the input string into chunks
    // lets remove the metamap to make it easier for us, as we won't need it anymore
    // convert the chunks to uppercase and save the files to the results directory


    split_ch
        .map { meta, files -> files }
        .flatten()
        .set { chunks_ch }

    CONVERTTOUPPER(chunks_ch)

}
