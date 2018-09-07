#################################################################
#
# Generate  documentation for the gsdm compoenent
#
# Written by DEIMOS Space S.L. (dibb)
#
# module gsdm
#################################################################
USAGE="Usage: `basename $0` -f pdf_file [-k]\n
Optional parameters:\n
-k: is set it indicates the script to keep the build directory
"
if [ -z "$GSDM_RESOURCES_PATH" ];
then
    echo -e "ERROR: The environment variable GSDM_RESOURCES_PATH has to be defined"
    echo -e $USAGE
    exit -1
fi

PDF_FILE=""
KEEP_BUILD="NO"

while getopts rf: option
do
    case "${option}"
        in
        f) PDF_FILE=${OPTARG};;
        r) KEEP_BUILD="YES";;
        ?) echo -e $USAGE
            exit -1
    esac
done

# Check that option -f has been specified
if [ "$PDF_FILE" == "" ];
then
    echo -e "ERROR: The option -f has to be provided"
    echo -e $USAGE
    exit -1
fi

# Create the build directory
if [ -d "build" ];
then
    rm -rf build
fi
mkdir build

# Automatically Extract the documentation from the python code
# Generate the tex file
sphinx-build -b latex ../../src/docs/source/ build
# Extract the chapter describing the code
sed -n '/chapter{Subpackages}/,/chapter{Module contents}/p' build/GSDM_module.tex |head -n -1 > gsdm_code_documentation.tex
# Replace the chapter text
sed -i 's/chapter{Subpackages}/chapter{GSDM code documentation}/' gsdm_code_documentation.tex

# Execute the first translation of the tex files into a PDF file
pdflatex -output-directory build -halt-on-error -interaction=nonstopmode doc.tex &> /dev/null

# Generate glossaries
makeglossaries -s build/doc.ist build/doc &> /dev/null

# Execute last translation to generate the PDF with the glossaries
pdflatex -output-directory build -halt-on-error -interaction=nonstopmode doc.tex |grep -i "warning\|error"

# Move generated pdf to its 
if [ -f build/doc.pdf ];
then
    mv build/doc.pdf $PDF_FILE
else
    echo -e "ERROR: There was an error on the generation of the PDF.\nIs pdflatex installed?"
fi

if [ "$KEEP_BUILD" != "YES" ];
then
    rm -rf build
fi
