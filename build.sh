set -e

CUR=`pwd`
for D in `find . -type d -name 'ch*'`
do
    DIR_CODE="$D/code"
    if [ ! -d $DIR_CODE ]; then
        echo "Skipping $DIR_CODE"
        continue
    fi

    cd $DIR_CODE
    echo "Building $DIR_CODE"

    make

    pytest *.py

    pwd
    cd $CUR
done