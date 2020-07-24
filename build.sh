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

    make -f ../../makefile clean
    make -f ../../makefile
    valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --error-exitcode=1 --exit-on-first-error=yes   ./build/gavran    

    pwd
    cd $CUR
done