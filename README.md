# ena-dl
Download FASTQ files from ENA
A fork of https://github.com/rpetit3/ena-dl

# Run with docker

Example:

`docker run --rm -it -v $PWD:/data quay.io/thanhleviet/end-dl ena-dl PRJEB2111 PRJEB2111 --is_study`

Reads of the study id PRJEB2111 will be downloaded into PRJEB2111 folder 
