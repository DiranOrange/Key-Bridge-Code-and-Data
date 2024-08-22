## Written by Diran Jimenez

# For more information on this script, see https://docs.google.com/document/d/193fWXqhxiuUAnnaMVHsQD4MCEMgZFoljzmf9WN_WzkE/edit?usp=sharing



##########################
### Importing Packages ###
##########################

# Packages for Handling Data
import pandas as pd
import numpy as np

# Packages for interfacing with internet
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

# Packages for parallel processing
from threading import Thread
import multiprocessing as mlt 




#########################
### Constructing URLs ###
#########################

days = ['0' + str(i) if i < 10 else str(i) for i in range(1,32)]

January = ['_01_' + i for i in days]
February = ['_02_' + i for i in days[:28]]
March = ['_03_' + i for i in days]
April = ['_04_' + i for i in days[:30]]
May = ['_05_' + i for i in days]
June = ['_06_' + i for i in days[:30]]
July = ['_07_' + i for i in days]
August = ['_08_' + i for i in days]
September = ['_09_' + i for i in days[:30]]
October = ['_10_' + i for i in days]
November = ['_11_' + i for i in days[:30]]
December = ['_12_' + i for i in days]
    # The underlines are to make the date fit into the final url
    
months = [January, February, March, April, May, June, July, August, September, October, November, December]

date_nums = [day for month in months for day in month]

# Next, the beginnning of each URL is the same for 2015 - 2023

base_url = 'https://coast.noaa.gov/htdata/CMSP/AISDataHandler/'

# Combine the date numbers with the general url format to get all URL's:

URLs = [base_url + str(Year) + '/AIS_' + str(Year) + Date + '.zip' for Year in range(2018, 2024) for Date in date_nums]
    # We don't consider 2015-2017 because the Transceiver Class was not tracked for this time
        # If a boat has Transceiver Class B, its data is very likely innacurate.
        
# This code was finalized in August 2024. As of writing, 2024 only has data for 01-03

current_months = January + February + March
current_urls = [base_url + '2024' + '/AIS_' + '2024' + Date + '.zip' for Date in current_months]

URLs += current_urls

# Can't forget about leap years!
URLs.append('https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2020/AIS_2020_02_29.zip')
URLs.append('https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_02_29.zip')
    # Funnily enough, Marine Cadatre DID forget about leap years
    # They do not list the 2024 leap year, as of writing, in their list but the URL still works and has data



############################
### Importing Boundaries ###
############################

# Information on what AIS information means:
    #https://documentation.spire.com/ais-fundamentals/different-classes-of-ais/ais-channel-access-methods/


# Read filters from the excel Sheet "Port_Boundaries" in this GitHub Repository    
port_data = pd.read_excel("https://raw.githubusercontent.com/DiranOrange/Key-Bridge-Code-and-Data/main/Port_Boundaries.xlsx", header = 0, index_col=0)

dict_ports = port_data.transpose().to_dict('list')

# Each port boundary is defined as an array of four vertices based on real world lat/lon data
port_boxes = {port: np.array(dict_ports[port]).reshape(5, 2) for port in dict_ports}


# Same method as port boundaries, but for bridges. Each bridge is defined by two points
bridge_data = pd.read_excel("https://raw.githubusercontent.com/DiranOrange/Key-Bridge-Code-and-Data/main/Bridge_Boundaries.xlsx", header=0, index_col = 0, usecols=["STRUCTURE_NAME", "START_X", "START_Y","END_X", "END_Y"], converters={"START_X":float, "START_Y":float, "END_X": float, "END_Y":float})
dict_bridges = bridge_data.transpose().to_dict('list')
bridge_lines = {bridge: np.array(dict_bridges[bridge]).reshape(2,2) for bridge in dict_bridges}


###################################################
### Writing to CSV Files (Multiprocessing Safe) ###
###################################################

'''
Each bridge gets its own csv file. To prevent processes from hanging by trying to write to the same csv
file, each file will get its own thread with a queue of data to write. This ensures a single file doesn't
get multiple simultaneous write requests.
'''

# WARNING!!!
# You must adjust the below folder path based on where you want to write data
# Always work out of the scr4 or scr16 folders
# WARNING!!!

data_folder = '/home/{your jhed}/scr4_mshiel10/{your usename}/{your folder}/'

# Example:
# data_folder = '/home/djimene9/scr4_mshiel10/djimenez/Bridge_Filtering_Data/'


# This function takes a dataframe out of a queue and writes it to a csv file
def writer(queue, bridge):
    # Keep writing until all dataframes have been processed
    while not queue.empty():
        
        # Remove the first dataframe from the que
        df = queue.get()
        
        # Write the dataframe to a bridge's csv file
        df.to_csv(data_folder + bridge + ' Data.csv', index=False, mode="a")
    return    

# Each writer is a separate thread, which handles one bridge in particular
    # If there were only a single thread for writing files, it would likely bottleneck the whole process


###########################
### Filtering Functions ###
###########################

# This was chosen arbitrarily according to group discussion
min_boat_length = 150
    # Units = meters


def filter_ports(file, boundaries, min_boat_length):
    """
    Parameters
    ----------
    file : path to the file to be filtered
        Zipfiles from the internet will be opened, and the path will be listed here
    
    boundaries : Dictionary of the form Port: Points Defining Port's Boundaries
        This function handles ports, while the other handles bridges. They are nearly identical, except
        each port is defined by four line segments, while bridges are only defined by one.
        
    min_boat_length : the smallest length a boat must be to be important
    
    Returns
    -------
    None. Data is written to an external file 
        The data consists of the points forming line segments from boats which intersect a port
        
    """
    
    # Each file is prepared as a data frame
        # To improve memory efficiency and read speeds, the datatype of each column is specified
    raw_data = pd.read_csv(file, sep=',', header=0, dtype={"Heading": "Int64",
                                                               "VesselName": str,
                                                               "IMO":str,
                                                               "MMSI":str, #Technically this should be an integer, but some files accidentally insert an alphanumeric character, causing a ValueError
                                                               "LAT":np.float64,
                                                               "LON":np.float64,
                                                               "SOG":np.float64,
                                                               "Heading":"Int64",
                                                               "COG":np.float64,
                                                               "IMO":str,
                                                               "CallSign":str,
                                                               "VesselType":"Int64",
                                                               "Status":"Int64",
                                                               "Length":"Int64",
                                                               "Width":"Int64",
                                                               "Cargo":"Int64",
                                                               "Draft":np.float64,
                                                               "TransceiverClass":str,
                                                               "TranscieverClass":str}, on_bad_lines="skip")

    

    # Keeping important AIS broadcast points based on three conditions:
    try:
        filtered = raw_data.loc[
            (((70 <= raw_data["VesselType"]) & (raw_data["VesselType"] < 90)) | (raw_data["VesselType"] == (1016 | 1017 | 1024 | 61)) | (raw_data["Length"] >= min_boat_length))
                # Ships must be longer than chosen minimum length (units are meters)
                # Or ships must be of class cargo, tanker, or cruise ship
                    # Vessel Type codes are based on the following: https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf
            
            & (((raw_data["SOG"] > 3) & (raw_data["Status"] != 1) & (raw_data["Status"] != 5)) | (raw_data["Status"] == (3 | 4)) )
                # ships must also be moving and not anchored and not moored, or moving with difficulty
            
            & (raw_data["TransceiverClass"] == "A")]
                # ships must also have transceiver class A (transceiver class B tends to give faulty information)
                
    except KeyError:
        filtered = raw_data.loc[
        (((70 <= raw_data["VesselType"]) & (raw_data["VesselType"] < 90)) | (raw_data["VesselType"] == (1016 | 1017 | 1024 | 61)) | (raw_data["Length"] >= min_boat_length) )
        & (((raw_data["SOG"] > 3) & (raw_data["Status"] != 1) & (raw_data["Status"] != 5)) | (raw_data["Status"] == (3 | 4)) )
        & (raw_data["TranscieverClass"] == "A")]
            # For some stupid reason there's occasionally a typo here
                # TranscIEver instead of TranscEIver
                
    # A list of all boats remaining after filtering        
    boats = filtered.drop_duplicates(["MMSI"])["MMSI"].values
    
    for port in boundaries:

            columns = filtered.columns.values.tolist()
            
            # Arrays can quickly be concatenated, great for combining chunks of data
            port_array = np.array([columns])
            
            # Data frames write to csv files quickly and robustly
            port_data = pd.DataFrame({})
            
            for i in range(4):
                # Each port consists of 4 line segments
                port_1_x = boundaries[port][i][0]
                port_1_y = boundaries[port][i][1]
                
                port_2_x = boundaries[port][i+1][0]
                port_2_y = boundaries[port][i+1][1]
                
                
                for boat in boats:
                    # Filter by each boat separately to prevent paths from getting wonky
                    # Additionally, organize chronologically to prevent paths from getting wonky
                    boat_broadcasts = filtered.loc[filtered["MMSI"] == boat].sort_values(["BaseDateTime"]).to_numpy()
                    
                    # Keep in mind these are vectors of x coordinates and y coordinates
                        # All calculations are performed as vectorized operations for speed
                    
                    boat_1_x = boat_broadcasts[:-1,3]
                    boat_1_y = boat_broadcasts[:-1,2]
                    
                    boat_2_x = boat_broadcasts[1:,3]
                    boat_2_y = boat_broadcasts[1:,2]
                    
                    # Be mindful of the indexing: if there are n boat points then there are n-1 line segments
                    
                    # a = segment boat_1 to boat_2
                    # b = segment port_1 to port_2
                    # c = segment boat_2 to port_1
                    # d = segment boat_2 to port_2
                    # e = segment port_2 to boat_1
                    
                    
                    a_x = boat_2_x - boat_1_x
                    a_y = boat_2_y - boat_1_y
                    
                    b_x = port_2_x - port_1_x
                    b_y = port_2_y - port_1_y
                    
                    c_x = port_1_x - boat_2_x
                    c_y = port_1_y - boat_2_y
                    
                    d_x = port_2_x - boat_2_x
                    d_y = port_2_y - boat_2_y
                    
                    e_x = boat_1_x - port_2_x
                    e_y = boat_1_y - port_2_y
                    
                    # The 2D cross product returns a scalar
                    # Perform it on the boat segment to each bridge point, and bridge segment to each boat point
                        # Hence, each cross product checks three points, since the end of one segment is the start of the second
                    
                    cross1 = a_x * c_y - a_y * c_x
                    cross2 = a_x * d_y - a_y * d_x
                    
                    cross3 = b_x * e_y - b_y * e_x
                    cross4 = b_y * d_x - b_x * d_y
                    
                        # cross4 should techincally be: b X (-d) {in english, b cross -d}
                        # However, the negative has been distributed to d X b, as is done above
                        # Hence, the swapping of x and y
                    
                    
                    # If the scalar is positive, the points are oriented counterclockwise (denoted 1)
                    # If the scalar is negative, the points are oriented clockwise (denoted -1)
                    # If the scalar is 0, the points are collinear
                    
                    o1 = np.where( (cross1 > 0), 1, 0)
                    O1 = np.where( (cross1 < 0), -1, o1)

                    o2 = np.where( (cross2> 0), 1, 0)
                    O2 = np.where( (cross2 < 0), -1, o2)

                    o3 = np.where( (cross3> 0), 1, 0)
                    O3 = np.where( (cross3 < 0), -1, o3)

                    o4 = np.where( (cross4 > 0), 1, 0)
                    O4 = np.where( (cross4 < 0), -1, o4)
                    
                    # When O1 and O2 have different signs, then the boat segment sees one bridge point on the left, and the other on the right
                        # As in, the orientation of the points is different
                        # Hence, it is intersected
                        
                    # However, the same must also be true from the bridge's perspective, otherwise the segments do not intersect
                        # See this page for a diagram: https://www.geeksforgeeks.org/check-if-two-given-line-segments-intersect/#

                    O_mask = np.where( ( (O1 != O2) & (O3 != O4) ), True, False )
                    
                    # Special Case: The end of the boat segment lies on the bridge segment
                        # True when:
                            # boat_2, bridge_1, and bridge_2 are collinear
                            # boat_2 lies between bridge_1 and bridge_2
                    
                    # To check if the boat lies between the bridge points, calculate the dot product of each bridge to the boat 
                    dot = d_x * c_x + d_y * c_y
                        # If the dot product is negative, the vectors point toward each other
                        # NOTE: To save on calculations the sign of both vectors is flipped, so if the dot product is negative the vectors actually point away from each other
                            # This is equivalent since the dot product only cares about the *angle* between vectors, which doesn't change when the sign of both is flipped
                        
                
                    C_mask = np.where( ( (O4 == 0) & (dot <= 0) ), True, False)
                        # (O4 == 0) checks that the points are collinear
                        # (dot <= 0) checks that the boat lies between the bridge points
                    
                    # Unfortunately, the data often has erroneous data points
                        # EX: Boat A goes from -125 lon to -123 lon and back to -125 in the span of a few seconds (hundreds of miles)
                        # EX: Boat B drops 4 hours of data and 'teleports' from 80 lat to 90 lat
                        # EX: Boat C is parked at -100 lon, 30 lat, but spontaneously teleports 5 degrees in random directions
                        
                    # To avoid counting these bad trips, if a boat makes too large a jump in one segment, ignore the segment
                    E_mask = np.where( (np.absolute(a_x) >= 1) | (np.absolute(a_y) >= 0.5), False, True)
                        # These values were chosen after a visual inspection of erroneous data points
                    
                    # Include segments that (intersect or have collinear points), and must NOT have a large jump between data points 
                    Final_mask = (O_mask | C_mask) & E_mask
                    
                    # Save each point of the intersecting line segments
                    boat_data1 = boat_broadcasts[:-1][Final_mask]
                    boat_data2 = boat_broadcasts[1:][Final_mask]
                    
                    # To make reconstructing line segments easier, points that form a line segment are stored next to each other
                    boat_data = np.empty(((boat_data1.shape[0]*2), boat_data1.shape[1]), dtype=boat_data1.dtype)
                    boat_data[0::2] = boat_data1
                    boat_data[1::2] = boat_data2
                    
                    # Don't attempt to write data to the port array if there isn't any new data
                    if boat_data.size != 0:
                        port_array = np.concatenate((port_array, boat_data))
                    
            # If there weren't any intersections with a port, don't attempt to write to its file        
            if len(port_array.shape) != 1:
                port_data = pd.DataFrame({columns[i]: port_array[1:,i] for i in range(len(columns))})
                port_data.to_csv('/home/djimene9/scr4_mshiel10/djimenez/Port_Filtering_Data/' + port + ' Data.csv')
    return



def filter_bridges(file, boundaries=bridge_lines, min_boat_length=min_boat_length):
    """
    Parameters
    ----------
    file : path to the file the be filtered
        Zipfiles from the internet will be opened, and the path will be listed here
    
    boundaries : Dictionary of the form Bridge: Points Defining Bridge's Boundaries
        This function handles bridges, while the other handles ports. They are nearly identical, except
        each port is defined by four line segments, while bridges are only defined by one.
        
    Returns
    -------
    None. Data is written to an external file 
        The data consists of the points forming line segments from boats which intersect a port
        
    """
    
    #Each file is prepared as a data frame
        #Specifying data types improves spead and memory
    raw_data = pd.read_csv(file, sep=',', header=0, dtype={"Heading": "Int64",
                                                               "VesselName": str,
                                                               "IMO":str,
                                                               "MMSI":str, #Technically this should be an integer, but some files accidentally insert an alphanumeric character, causing a ValueError
                                                               "LAT":np.float64,
                                                               "LON":np.float64,
                                                               "SOG":np.float64,
                                                               "Heading":"Int64",
                                                               "COG":np.float64,
                                                               "IMO":str,
                                                               "CallSign":str,
                                                               "VesselType":"Int64",
                                                               "Status":"Int64",
                                                               "Length":"Int64",
                                                               "Width":"Int64",
                                                               "Cargo":"Int64",
                                                               "Draft":np.float64,
                                                               "TransceiverClass":str}, on_bad_lines="skip")

    # Keeping important AIS broadcast points
    try:
        filtered = raw_data.loc[
        (((70 <= raw_data["VesselType"]) & (raw_data["VesselType"] < 90)) | (raw_data["VesselType"] == (1016 | 1017 | 1024 | 61)) | (raw_data["Length"] >= min_boat_length) )
            # Ships must be longer than chosen minimum length (units are meters)
            # Or ships must be of class cargo, tanker, or cruise ship
                # Vessel Type codes are based on the following: https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf
            
        & (((raw_data["SOG"] > 3) & (raw_data["Status"] != 1) & (raw_data["Status"] != 5)) | (raw_data["Status"] == (3 | 4)) )
            # ships must also be moving and not anchored and not moored, or moving with difficulty
            
        & (raw_data["TransceiverClass"] == "A")]
            # ships must also have transceiver class A (transceiver class B tends to give faulty information)
    
    except KeyError:
        filtered = raw_data.loc[
        (((70 <= raw_data["VesselType"]) & (raw_data["VesselType"] < 90)) | (raw_data["VesselType"] == (1016 | 1017 | 1024 | 61)) | (raw_data["Length"] >= min_boat_length) )
        & (((raw_data["SOG"] > 3) & (raw_data["Status"] != 1) & (raw_data["Status"] != 5)) | (raw_data["Status"] == (3 | 4)) )
        & (raw_data["TranscieverClass"] == "A")]
            # For some stupid reason there's occasionally a typo here
                # TranscIEver instead of TranscEIver
                
                
    # A list of all boats remaining after filtering        
    boats = filtered.drop_duplicates(["MMSI"])["MMSI"].values
    
    for bridge in boundaries:
            
            columns = filtered.columns.values.tolist()
                
            bridge_array = np.array([columns])
            bridge_data = pd.DataFrame({})
            
            # Each bridge is described by five points to represent its curvature
                # However, filtering only requires a single line segment, which will be reasonably accurate
                # This line segment is defined by the first and last points of the bridge
                
            bridge_1_x = boundaries[bridge][0][0]
            bridge_1_y = boundaries[bridge][0][1]
            
            
            bridge_2_x = boundaries[bridge][-1][0]
            bridge_2_y = boundaries[bridge][-1][1]
                
            
            for boat in boats:
                # Filter by each boat separately to prevent paths from getting wonky in the transition between boats
                # Additionally organize chronologically to prevent paths from getting wonky
                boat_points = filtered.loc[filtered["MMSI"] == boat].sort_values(["BaseDateTime"]).to_numpy()
                
                # Keep in mind these are vectors of x coordinates and y coordinates
                    # All calculations are performed as vectorized operations for speed
                boat_1_x = boat_points[:-1,3]
                boat_1_y = boat_points[:-1,2]
                
                boat_2_x = boat_points[1:,3]
                boat_2_y = boat_points[1:,2]
                # Be mindful of the indexing: if there are n boat points then there are n-1 line segments
                
                
                # Pre-calculate all necessary line segments
                # a = segment boat_1 to boat_2
                # b = segment bridge_1 to bridge_2
                # c = segment boat_2 to bridge_1
                # d = segment boat_2 to bridge_2
                # e = segment bridge_2 to boat_1

                
                a_x = boat_2_x - boat_1_x
                a_y = boat_2_y - boat_1_y
                
                b_x = bridge_2_x - bridge_1_x
                b_y = bridge_2_y - bridge_1_y
                
                c_x = bridge_1_x - boat_2_x
                c_y = bridge_1_y - boat_2_y
                
                d_x = bridge_2_x - boat_2_x
                d_y = bridge_2_y - boat_2_y
                
                e_x = boat_1_x - bridge_2_x
                e_y = boat_1_y - bridge_2_y
                
                # The 2D cross product returns a scalar
                # Perform it on the boat segment to each bridge point, and bridge segment to each boat point
                    # Hence, each cross product considers three points, since the end of one segment is the start of the second
                
                cross1 = a_x * c_y - a_y * c_x
                cross2 = a_x * d_y - a_y * d_x
                
                cross3 = b_x * e_y - b_y * e_x
                cross4 = b_y * d_x - b_x * d_y
                    # This should be: b X (-d) (aka b cross -d)
                    # However, the negative has been distributed to d X b, as is done above
                
                # If the scalar is positive, the points are oriented counterclockwise (denoted 1)
                # If the scalar is negative, the points are oriented clockwise (denoted -1)
                # If the scalar is 0, the points are collinear
                
                o1 = np.where( (cross1 > 0), 1, 0)
                O1 = np.where( (cross1 < 0), -1, o1)

                o2 = np.where( (cross2 > 0), 1, 0)
                O2 = np.where( (cross2 < 0), -1, o2)

                o3 = np.where( (cross3 > 0), 1, 0)
                O3 = np.where( (cross3 < 0), -1, o3)

                o4 = np.where( (cross4 > 0), 1, 0)
                O4 = np.where( (cross4 < 0), -1, o4)
                
                
                # When O1 and O2 have different signs, then the boat segment sees one bridge point on the left, and the other on the right
                    # As in, the orientation of the points is different
                    # Hence, it is intersected
                    
                # However, the same must also be true from the bridge's perspective, otherwise the segments do not intersect
                    # See this page for a diagram: https://www.geeksforgeeks.org/check-if-two-given-line-segments-intersect/#

                O_mask = np.where( ( (O1 != O2) & (O3 != O4) ), True, False )
                
                # Special Case: The end of the boat segment lies on the bridge segment
                    # True when:
                        # boat_2, bridge_1, and bridge_2 are collinear
                        # boat_2 lies between bridge_1 and bridge_2
                
                # To check if the boat lies between the bridge points, calculate the dot product of each bridge to the boat 
                dot = d_x * c_x + d_y * c_y
                    # If the dot product is negative, the vectors point toward each other
                    # NOTE: To save on calculations the sign of both vectors are flipped, so if the dot product is negative the vectors actually point away from each other
                        # This is equivalent, since the dot product only cares about the *angle* between vectors, which doesn't change when the sign of both is flipped
                    
                C_mask = np.where( ( (O4 == 0) & (dot <= 0) ), True, False)
                    # (O4 == 0) checks that the points are collinear
                    # (dot <= 0) checks that the boat lies between the bridge points
                
                # Unfortunately, the data often has erroneous data points
                    # EX: Boat A goes from -125 lon to -123 lon and back to -125 in the span of a few seconds
                    # EX: Boat B drops 4 hours of data and 'teleports' from 80 lat to 90 lat
                    # EX: Boat C is parked at -100 lon, 30 lat, but spontaneously teleports 5 degrees in random directions
                    
                # To avoid counting these bad trips, if a boat makes too large a jump in one segment, ignore the segment
                E_mask = np.where( (np.absolute(a_x) >= 1) | (np.absolute(a_y) >= 0.5), False, True)
                
                # Include segments that (intersect or have collinear points), and must NOT have a large jump between data points 
                Final_mask = (O_mask | C_mask) & E_mask
                
                # Save each point of the intersecting line segments
                boat_data1 = boat_points[:-1][Final_mask]
                boat_data2 = boat_points[1:][Final_mask]
                
                # To make reconstructing line segments easier, points that form a line segment are stored next to each other
                boat_data = np.empty(((boat_data1.shape[0]*2), boat_data1.shape[1]), dtype=boat_data1.dtype)
                boat_data[0::2] = boat_data1
                boat_data[1::2] = boat_data2
                
                
                # Don't attempt to write data to the bridge array if there isn't any new data
                if boat_data.size != 0:
                    bridge_array = np.concatenate((bridge_array, boat_data))
                    
            # If there weren't any intersections with a bridge, don't attempt to write to its file
            if len(bridge_array.shape) != 1:
                bridge_data = pd.DataFrame({columns[i]: bridge_array[1:,i] for i in range(len(columns))})
                #Put the bridges data into its que
                queues[bridge].put(bridge_data)
                
    return                
       

    
############################        
### Downloading the Data ###
############################

# Go through each URL, open the file, then apply the filtering function    

def download_and_filter_bridges(url):
    # Download the file from the link so it can be interacted with
    download = urlopen(url)
    
    # Unzip the file
    data_zip_file = ZipFile(BytesIO(download.read()))
    
    #The actual file name is derived from the url, and looks like AIS_Year_DateNum
    file_name = url[55:-4] + '.csv'
        #The first 50 characters are the base url, the last 4 are .zip
    
    #Apply the filter function to the file
    try:
        filter_bridges(data_zip_file.open(file_name), queues)
    except Exception as e:
        # If there's any error, rockfish will completely halt
        # Instead, ignore the error for later so the entire job doesn't get wasted
        print(f"File {file_name} failed, need to refilter! \n Error: {e}", flush=True)
    return 

def download_and_filter_ports(url):
    # Download the file from the link so it can be interacted with
    download = urlopen(url)
    # Unzip the file
    data_zip_file = ZipFile(BytesIO(download.read()))
    
    #The actual file name is derived from the url, and looks like AIS_Year_DateNum
    file_name = url[55:-4] + '.csv'
        #The first 50 characters are the base url, the last 4 are .zip
    
    #Apply the filter function to the file
    try:
        filter_ports(data_zip_file.open(file_name), boundaries=port_boxes, min_boat_length=150)
    except Exception as e:
        print(f"File {file_name} failed, need to refilter! \n Error: {e}", flush=True)
    return 




########################
### Running the code ###
########################

if __name__ == "__main__":
    
    print("Script Began")
    
    files = URLs
        # In the case that the script halts prematurely, slice the list of URLs past every file in a batch that was completed
        # Keep in  mind, the data from filtered files is not written until ALL files have been finished
        # As in, if the next batch didn't start, the previous batch has not been written and must be redone
        
    num_files = len(files)
    
    # The number of cores represents the max number of processes running simultaneously
        # Memory errors can occur if there are very few spare cores in the rockfish allocation
        # TDL: look into utilizing big mem partition
    num_cores = 36
        # This code was ran using a max of 38 cores on 2 nodes of 48 cores each (96 cores total) and took approximately 27 hours
    
    # Due to memory constraints, the files must be handled in batches. Reps is the number of batches, num_cores the size of each batch
    reps = int(num_files/num_cores)
    
    # The number of cores will almost certainly not properly divide the number of files
        # Thus, process the remaining files in a final batch
    remainder = num_files - (reps * num_cores)

    # A single manager will be used to coordinate all processes throughout the job
    with mlt.Manager() as manager:
        # A manager queue is can be shared safely across all processes
        queues = {bridge:manager.Queue() for bridge in bridge_lines}
            # Rather than manually create a queue for each bridge, they are all stored in a dictionary
            
            
        # Create a pool with max processes = num_cores
        with mlt.Pool(num_cores) as pool:
            for i in range(reps):
                # starmap applies multiple arguments to a function
                    # Here, download and filter is run on each batch of files, connected to the managed queues
                pool.starmap(download_and_filter_bridges, [(url,queues) for url in files[num_cores * i:num_cores * (i+1)]])
                    # Note: Starmap implicitly joins all processes
                        # As in, the rest of the code in the main file will wait until all processes are complete
                        
                # Create a thread for each bridge that will run the writer argument 
                writers = {bridge:Thread(target=writer, args=[queues[bridge], bridge]) for bridge in bridge_lines}
                
                # Start all writers, and wait for them all to finish
                for bridge in bridge_lines:
                    writers[bridge].start()
                    writers[bridge].join()
                    # Once all files have been written, begin the next batch

            # Finally, perform the same process, but for the last, irregular batch
            pool.starmap(download_and_filter_bridges, [(url, queues) for url in files[-remainder:]])
            writers = {bridge:Thread(target=writer, args=[queues[bridge], bridge]) for bridge in bridge_lines}

            for bridge in bridge_lines:
                writers[bridge].start()
                writers[bridge].join()
  
            

'''
Potential improvement for multiprocessing:
    
In an ideal world, each thread for writing is constantly running, while new processes are smoothly 
started as older ones finish, rather than working in chunks. To make this happen, you would
need to adjust the writer function to run continuously and somehow maintain a counter for the
number of files you've written, including any potential errors that stop a file from being written,
otherwise each thread would never halt. However, constantly checking if a queue is empty is 
very slow, as it requires obtaining the mutex for each queue on each check. The solution in this 
file works, so I've opted to leave improving it to someone else who hopefully has more experience
with multiprocessing. 

The purpose of handling all files in batches is due to memory issues. If there were sufficient
memory, all files could be handled simultaneously, and everything could be written at the very end. 
However, I have no idea how to ensure memory is non-binding, especially because failing to save work
progressively could mean it's all lost in case of an error. The following link goes into a deep dive
on memory usage while working in pandas and multiprocessing: https://stackoverflow.com/questions/49429368/how-to-solve-memory-issues-while-multiprocessing-using-pool-map 
NOTE: Running this script with 36 cores on one node with 48 total cores lead to no memory issues.
However, running this script with 72 cores on two nodes with 96 cores total DOES lead to a memory error.
'''
    