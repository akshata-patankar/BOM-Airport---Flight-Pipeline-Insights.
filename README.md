### BOM Airport - Flight Pipeline & Insights.
BOM Airport (Chhatrapati Shivaji Maharaj International Airport - BOM is the IATA code.) is one of India’s busiest airports, handling hundreds of flights daily, both domestic and international. Efficiently managing flight schedules, predicting delays, and understanding patterns in arrivals and departures is crucial for airlines, airport authorities, and logistics planning.

This projects includes a complete ETL pipeline, Dasboard development and ML models which provides all necessary to means to obtain actionable insights for airport operations.

**ETL Pipeline---------------------------------------------------------------------------------**

* Extract: Pulls arrival and departure flight data from the AviationStack API.
* Clean: Removes unnecessary columns like baggage, gates, and codeshare information.
* Merge & Transform: Combines arrivals and departures into a single dataset and converts time columns to datetime.
* Enrich: Adds origin and destination country information using the OpenFlights airport dataset.
* Feature Engineering: Calculates additional columns like departure_hour & arrival_hour, departure_delay_flag & arrival_delay_flag, total_delay, flight_type (Domestic/International) and flight_duration in minutes
* Flag Missing Values: Adds boolean flags for missing delays, times, flight duration, and total delay.
* Output: Produces a clean, enriched dataset ready for EDA, Dashboard development, and machine learning.

**Note** - The get_data() process has been skipped in the ETL pipeline due to API constraints (limits on data fetching) . The raw flight data was pre-fetched and saved locally to ensure smooth execution of the pipeline.

**Data Visualization--------------------------------------------------------------------------**

Dashboard 1:

* Overview of arrivals and departures
* Visualizes flight types, airline performance, and delay KPIs
* Shows average delays and early/on-time/delayed flights

Dashboard 2 (in progress):

* Focus on delay categories (Early, On-Time, Delayed)
* Tracks airline-wise delay trends
* Summarizes total delay insights for operational planning

Purpose: Provides interactive visuals for logistics and airport operations, helping stakeholders quickly understand flight performance.

**Machine Learning----------------------------------------------------------------------------**

Random Forest Classification:

* Predicts flight status (Early, On-Time, Delayed)
* Handles categorical features via frequency encoding for high-cardinality columns
* Achieves ~86-88% train/test accuracy, showing good generalization
