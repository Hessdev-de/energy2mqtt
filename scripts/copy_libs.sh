#!/bin/sh
if [ -z "$1" ]; then
	echo "Need to specify binary"
	exit 1
fi

echo "Libs for $1"
for i in `ldd $1 | awk -F'=>' '{print $2}' | awk -F' ' '{print $1}' | grep '^/'`; do
	mkdir -p /build/`dirname $i`
	echo "  $i => /build/$i"
	cp $i /build/$i
	$0 $i
done
# handle ld

LD=`ldd $1 | grep ld-linux | awk -F ' ' '{print $1}'`
echo "LD: $LD"
mkdir -p /build/`dirname $LD`
cp $LD /build/`dirname $LD`


cp /usr/bin/ldd /build/usr/bin/ldd

