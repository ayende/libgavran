param([string]$type = "pdf", [boolean]$start = $true)

if ($start) {
	Remove-Item  Output -Force -Recurse -ErrorAction SilentlyContinue | out-null 
}
MkDir Output -ErrorAction SilentlyContinue  | out-null


$output = ".\Output\StorageEngine.$type"

pandoc --table-of-contents --toc-depth=3 --epub-metadata=metadata.xml --reference-links `
	--standalone --highlight-style=espresso --self-contained --top-level-division=chapter `
	--listings --pdf-engine=xelatex --number-sections --css=pandoc.css `
	-o $output .\ch01\ch01.md .\ch02\ch02.md .\ch03\ch03.md

if($start) {
	start $output
}

return $output