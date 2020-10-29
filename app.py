import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Welcome to my Hawaii Climate Analysis API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/temp/startdate/enddate"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the precipitation data for the last year"""
    session = Session(engine)
    # Calculate the date 1 year ago from last date in database
    one_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    # Query for the date and precipitation for the last year
    precipitation_query = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year).all()
    session.close()

    # Dict with date as the key and prcp as the value
    data = {date: prcp for date, prcp in precipitation_query}
    return jsonify(data)


@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    """Return a list of stations."""
    station_query = session.query(Station.name).all()
    session.close()

    # Unravel results into a 1D array and convert to a list
    stations = list(np.ravel(station_query))
    return jsonify(stations=stations)


@app.route("/api/v1.0/tobs")
def temps():
    session = Session(engine)
    """Return the temperature observations (tobs) for previous year."""
    # Calculate the date 1 year ago from last date in database
    one_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    # Query the primary station for all tobs from the last year
    query_4 = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()

    most_active = query_4[0][0]
    
    temp = session.query( Measurement.tobs).filter(Measurement.station == most_active).filter(Measurement.date >= one_year).all()
    session.close()

    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(temp))

    # Return the results
    return jsonify(temps=temps)


@app.route("/api/v1.0/temp/<startdate>")
@app.route("/api/v1.0/temp/<startdate>/<enddate>")
def statistics(startdate=None, enddate=None):
    """Return TMIN, TAVG, TMAX."""
    
  
    if not enddate:
        session = Session(engine)
        # calculate TMIN, TAVG, TMAX for dates greater than start
        query_5 = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= startdate).all()
        session.close()
        # Unravel results into a 1D array and convert to a list
        temps = list(np.ravel(query_5))
        return jsonify(temps=temps)
        

    # calculate TMIN, TAVG, TMAX with start and stop
    session = Session(engine)
    query_6 = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= startdate).filter(Measurement.date <= enddate).all()
    session.close()
    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(query_6))
    return jsonify(temps=temps)      


if __name__ == '__main__':
    app.run()
