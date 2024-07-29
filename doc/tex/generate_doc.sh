#################################################################
#
# Generate  documentation for the eboa component
#
# Written by DEIMOS Space S.L. (dibb)
#
# module eboa
#################################################################
USAGE="Usage: `basename $0` -f pdf_file [-k]\n
Optional parameters:\n
-k: if set it indicates the script to keep the build directory
"
if [ -z "$EBOA_RESOURCES_PATH" ];
then
    echo -e "ERROR: The environment variable EBOA_RESOURCES_PATH has to be defined"
    echo -e $USAGE
    exit -1
fi

PDF_FILE=""
KEEP_BUILD="NO"

while getopts kf: option
do
    case "${option}"
        in
        f) PDF_FILE=${OPTARG};;
        k) KEEP_BUILD="YES";;
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
# Add chapter part
echo "\chapter{EBOA code documentation}" > eboa_code_documentation.tex
# Extract the chapter describing the code
sed -n '/\\section{Subpackages}/,/\\section{Module contents}/p' build/EBOA_module.tex |head -n -1 >> eboa_code_documentation.tex

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
    echo -e "\nSUCCESS: The PDF was correctly generated with the name: "$PDF_FILE"\n"
else
    echo -e "ERROR: There was an error on the generation of the PDF.\nIs pdflatex installed?"
fi

if [ "$KEEP_BUILD" != "YES" ];
then
    rm -rf build
fi
