param([string]$type = "pdf", [boolean]$start = $true)

if ($start) {
	Remove-Item  Output -Force -Recurse -ErrorAction SilentlyContinue | out-null 
}
MkDir Output -ErrorAction SilentlyContinue  | out-null


$output = ".\Output\StorageEngine.$type"

pandoc --table-of-contents --toc-depth=3 --epub-metadata=metadata.xml `
	--standalone --highlight-style=espresso --self-contained --top-level-division=part `
	--listings --pdf-engine=xelatex --number-sections --css=pandoc.css `
    #--epub-cover-image=.\Cover.jpg
    #--epub-embed-font=Styling/RobotoMono-Regular.ttf `
	-o $output .\ch01\ch01.md

if($start) {
	start $output
}

return $output