# Matthew Lett
import numpy as np
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.sql import exists

from flask import Flask, jsonify
from sqlalchemy.sql.expression import desc


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date (YYYY-MM-DD)<br/>"
        f"/api/v1.0/start_date/end_date (YYYY-MM-DD)/(YYYY-MM-DD)"
    )

# Precipitation Route
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Convert the Query Results to a Dictionary Using `date` as the Key and `prcp` as the Value

    # Create our session (link) from Python to the DB
    session = Session(engine)    
      
    # Get previous year's date
    previous_year_date = dt.date(2017,8,23) - dt.timedelta(days=365)

    prcp_data = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= previous_year_date).\
        order_by(Measurement.date).all()
    
    # Convert List of Tuples Into a Dictionary
    prcp_data_list = dict(prcp_data)
    
    # Return JSON Representation of Dictionary
    return jsonify(prcp_data_list)

# Station Route
@app.route("/api/v1.0/stations")
def stations():
     # Create our session (link) from Python to the DB
    session = Session(engine)

    # Return a JSON List of Stations From the Dataset
    all_stations = session.query(Station.station, Station.name).all()
    
    # Convert List of Tuples Into Normal List
    station_list = list(all_stations)
    
    # Return JSON List of Stations from the Dataset
    return jsonify(station_list)

# TOBs Route
@app.route("/api/v1.0/tobs")
def tobs():
     # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query the dates and temperature observations of the most active station for the last year of data.
    previous_year_date = dt.date(2017,8,23) - dt.timedelta(days=365)

    # Retrieve the station with the most observations
    hoc_station_query = session.query(Measurement.station, func.count(Measurement.station)) \
        .group_by(Measurement.station) \
        .order_by(func.count(Measurement.station).desc()) \
        .all()
    
    station = hoc_station_query[0][0]
    
    # Retrieve the Last 12 Months of Precipitation Data Selecting Only the `date` and `prcp` Values
    sel = [Station.name, Station.station, Measurement.date, Measurement.tobs]
    tobs_data = session.query(*sel).filter(Station.station ==  Measurement.station) \
        .filter(Measurement.date >= previous_year_date) \
        .filter(Measurement.station == station) \
        .order_by(Measurement.date).all()

    # Convert List of Tuples Into Normal List
    tobs_list = list(tobs_data)
    
    # Return JSON List of Temperature Observations (tobs) for the Previous Year
    return jsonify(tobs_list)

# Start Date Route
@app.route("/api/v1.0/<start_date>")
def start_date(start_date):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Check for a valid entry
    valid_entry = session.query(exists().where(Measurement.date == start_date)).scalar()

    if valid_entry:

        # Query for retrieviing the data beginning with the starting date
        starting_date = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)) \
            .filter(Measurement.date >= start_date) \
            .group_by(Measurement.date).all()

        # Convert List of Tuples Into Normal List
        start_day_list = list(starting_date)
        
        # Return JSON List of Min Temp, Avg Temp and Max Temp for a Given Start Range
        return jsonify(start_day_list)
    
    return jsonify({"error": f"Input date {start_date} is not valid."}), 404

# Start / End Date Route
@app.route("/api/v1.0/<start_date>/<end_date>")
def start_end_date(start_date,end_date):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Check for a valid entry
    valid_entry_start = session.query(exists().where(Measurement.date == start_date)).scalar()

    if valid_entry_start:

        #Check for a valid entry
        valid_entry_end = session.query(exists().where(Measurement.date == end_date)).scalar()

        if valid_entry_end:

            # Query to retrieve the data within the requested dates
            date_data = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)) \
                .filter(Measurement.date >= start_date) \
                .filter(Measurement.date <= end_date) \
                .group_by(Measurement.date).all()

            # Convert List of Tuples Into Normal List
            date_list = list(date_data)
            
            # Return JSON List of Min Temp, Avg Temp and Max Temp for a Given Start Range
            return jsonify(date_list)

        return jsonify({"error": f"Input date {end_date} is not valid."}), 404

    return jsonify({"error": f"Input date {start_date} is not valid."}), 404

if __name__ == "__main__":
    app.run(debug=True)