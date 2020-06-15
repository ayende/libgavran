rm -r ./build
mkdir ./build 
asciidoctor -r asciidoctor-diagram --destination-dir ./output  .\book.adoc
./output/book.html