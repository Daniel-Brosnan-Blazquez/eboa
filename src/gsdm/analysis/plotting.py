"""
Data analysis using openpyxl for generating excel 2010 workbenches

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import matplotlib
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as plt_dates

# Import python utilities
import numpy
import tempfile

def generate_gantt(y_labels, data):
    """
    """
    fig = plt.figure(figsize=(12,8))
    ax = fig.add_subplot(111)
    
    i = 1
    for item in data:
        if item["start"] != None and item["stop"] != None:
            color = "blue"
            if "color" in item:
                color = item["color"]
            # end if
            barh = ax.barh(i*0.5, plt_dates.date2num(item["stop"]) - plt_dates.date2num(item["start"]), left=plt_dates.date2num(item["start"]), height=0.3, align='center', color=color, alpha = 0.75)
            if "text" in item:
                ax.annotate(item["text"], (plt_dates.date2num(item["start"])+0.001, (i*0.5)-0.15))
            # end if
            i += 1
        # end if
    # end for
    
    pos = numpy.arange(0.5,i,0.5)
    locsy, labelsy = plt.yticks(pos,y_labels)
    plt.setp(labelsy, fontsize = 8)
    ax.grid(color = 'g', linestyle = ':')
    ax.set_ylim(ymin = -0.1, ymax = i*0.5)
    
    # Set x-axis as dates
    formatter = plt_dates.DateFormatter("%Y-%m-%dT%H:%M:%S")
    ax.xaxis.set_major_formatter(formatter)
    ax.xaxis_date()
    ax.invert_yaxis()
    fig.autofmt_xdate()
    plt.tight_layout()
    new_file, filename = tempfile.mkstemp(suffix=".jpg")
    plt.savefig(filename)
    
    return filename
    



