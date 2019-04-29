# Installation script for IRIS Seed tools
#
# 	dataselect-3.21 (https://github.com/iris-edu/dataselect/releases)
# 	libmseed-2.19.6 (https://github.com/iris-edu/libmseed/releases)
# 	msi-3.8 (https://github.com/iris-edu/msi/releases)
#
# The source code should be unpacked in the ./lib directory
#
# Usage:
#
#	./install.sh 
# 	./install.sh clean
#

makelib () {

	# Create or remove bin directory
	if [ ! -d "bin" ]; then
		mkdir bin
	elif [ "$1" = "clean" ]; then
		rm -r bin
	fi

	# Go over all IRIS libs
	for dir in lib/*; do
	
	        lib=$(basename ${dir})
	
		echo "Building ${lib} $1."
	
		# Make and redirect stdout to /dev/null
		case ${lib} in
			"dataselect-3.21")
				make -C ${dir} $1 > /dev/null
			;;
			"libmseed-2.19.6")
				make -C ${dir}/example $1 > /dev/null
			;;
			"msi-3.8")
				make -C ${dir} $1 > /dev/null
			;;
		esac
	
		# Show success or failure
		if [ $? -ne 0 ]; then
			echo "Error builing ${lib}."
			exit 1
		fi

		# A clean is requested: stop here
		if [ "$1" = "clean" ]; then
			continue
		fi

		echo "Succesfully built ${lib}. Copying binary file to bin."

	        # Copy executables to the bin directory
	        case ${lib} in
	                "dataselect-3.21")
	                        cp ${dir}/dataselect bin
	                ;;
	                "libmseed-2.19.6")
	                        cp ${dir}/example/msrepack bin
	                ;;
	                "msi-3.8")
	                        cp ${dir}/msi bin
	                ;;
	        esac
	
	done

}

# Command line options
case $1 in
        "clean")
                makelib clean
		echo "Done cleaning."
        ;;
        *)
                makelib all
		echo "Done building IRIS tools. Add ./bin to \$PATH."
        ;;
esac

exit 0
