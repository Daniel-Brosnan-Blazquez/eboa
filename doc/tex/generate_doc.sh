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
