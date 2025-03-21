import datetime
import folium
import psycopg2
import geopandas as gpd
import os
import logging
from dotenv import load_dotenv
import pandas as pd
from shapely.geometry import Point, LineString
from pathlib import Path
from folium.plugins import PolyLineTextPath, Fullscreen
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # Database connection parameters from environment variables
    host = os.getenv('psgrsql_db_host', "")
    dbname = os.getenv('psgrsql_db_name', "")
    user = os.getenv('psgrsql_db_user', "")
    password = os.getenv('psgrsql_db_pswd', "")
    port = os.getenv('psgrsql_db_port', "5432")

    def get_connection():
        """Establishes and returns a database connection."""
        try:
            conn = psycopg2.connect(
                host=host,
                dbname=dbname,
                user=user,
                password=password,
                port=port
            )
            return conn
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")
            return None

    def fetch_route_numbers(site_id):
        """
        Fetch route numbers from the database for the date two days prior to today.
        Returns:
            list: A list of route numbers if the query is successful, or an empty list if an error occurs.
        """
        conn = get_connection()  # Establish a database connection
        if not conn:
            logging.error("Failed to connect to the database.")
            return []  # Return an empty list to avoid errors downstream
        query = f"""
            SELECT route_no
            FROM v_onroute_charge_daily_plan
            WHERE plan_departure_time::date = CURRENT_DATE - INTERVAL '2 days' AND site_id={site_id};
        """
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            # Retrieve all rows as tuples
            rows = cursor.fetchall()
            # Use an inline safety check for tuple processing (ensure it's safe to fetch indices)
            route_nos = [row[0] for row in rows if len(row) > 0]  # Index 0 fetches 'route_no'
            logging.info(f"Successfully fetched {len(route_nos)} route numbers.")
            return route_nos
        except Exception as e:
            logging.error(f"Error fetching route numbers: {e}")
            return []  # Graceful fallback on query failure
        finally:
            conn.close()  # Ensure the database connection is closed

    def get_vehicle_and_route_info(route_id, conn):
        """
        Part 1 - Find the vehicle and route alias:
        - go to t_route_plan in PREPROD
        - search by using the route_id from the csv file
        - return the vehicle_id AND the route_alias
        - if the vehicle_id === X stop the function (vehicle is not dartfort or registered)
        """
        try:
            query = f"""
                SELECT vehicle_id, route_alias 
                FROM public.t_route_plan 
                WHERE route_id = '{route_id}'
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                logging.error(f"No route plan found for route_id: {route_id}")
                return None, None
            
            vehicle_id = df.iloc[0]['vehicle_id']
            route_alias = df.iloc[0]['route_alias']
            
            return vehicle_id, route_alias
            
        except Exception as e:
            logging.error(f"Error getting vehicle and route info: {e}")
            return None, None
    
    def get_route_timing(current_date_minus_1, vehicle_id, route_alias, conn):
        """
        Part 2 - Find the actual start and end date time for the route:
        - go to t_route_data_from_telematics in PREPROD
        - search by using the route_alias and route_start_time
        - return route_start_time and route_end_time
        - if no rows stop the function (we don't have any telematics data for vehicle)
        """
        try:
            query = f"""
                SELECT route_start_time, route_end_time
                FROM public.t_route_data_from_telematics
                WHERE route_alias = '{route_alias}' AND vehicle_id = '{vehicle_id}' AND route_start_time::date = '{current_date_minus_1}'
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                logging.error(f"No telematics data found for route_alias: {route_alias}")
                return None, None
                
            route_start_time = df.iloc[0]['route_start_time']
            route_end_time = df.iloc[0]['route_end_time']
            
            return route_start_time, route_end_time
            
        except Exception as e:
            logging.error(f"Error getting route timing: {e}")
            return None, None
    
    def get_telematics_data(vehicle_id, route_start_time, route_end_time, conn):
        """
        Part 3 - Find the geolocation of the vehicle based on actual route_start_time and route_end_time
        - go to public.stg_masternaut_last_n_days in PREPROD
        - return the location data while filtering by vehicle_id AND the 'date' by > route_start_time AND < route_end_time
        - if there is no location data stop the function (location data is faulty)
        """
        try:
            query = f"""
                SELECT vehicle_id, latitude, longitude, date, speed
                FROM public.stg_masternaut_last_n_days
                WHERE vehicle_id = '{vehicle_id}'
                AND date BETWEEN '{route_start_time}' AND '{route_end_time}'
                ORDER BY date
            """
            df = pd.read_sql(query, conn)
            
            if df.empty:
                logging.error(f"No location data found for vehicle {vehicle_id} during time range")
                return None
                
            return df
            
        except Exception as e:
            logging.error(f"Error getting telematics data: {e}")
            return None
    
    def create_pdf_report(output_dir, current_date_minus_1):
        """
        Creates a PDF with screenshots of all maps and a legend explaining the markers and lines.
        
        Args:
            output_dir (str): Directory containing the HTML map files
            pdf_filename (str): Name of the output PDF file
        
        Returns:
            str: Path to the created PDF or None if there was an error
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            import time
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib import colors
            from reportlab.lib.units import inch
            import glob
            import re
        except ImportError as e:
            logging.error(f"Missing required libraries: {e}")
            logging.error("Please install: selenium, webdriver-manager, reportlab")
            logging.error("pip install selenium webdriver-manager reportlab")
            return None
        
        # Set up Chrome options for headless browser
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Try to locate Chrome in various locations
        chrome_paths = [
            '/usr/bin/chromium-browser',
            '/usr/bin/chromium',
            '/usr/bin/google-chrome',
            '/usr/bin/google-chrome-stable'
        ]
        
        for path in chrome_paths:
            if os.path.exists(path):
                chrome_options.binary_location = path
                logging.info(f"Using Chrome binary at {path}")
                break
        
        # Create a webdriver
        try:
            driver = webdriver.Chrome(options=chrome_options)
        except Exception as e:
            logging.error(f"Error creating Chrome webdriver: {e}")
            return None
        
        # Create the PDF document
        pdf_filename = f'route_map_report_{current_date_minus_1}.pdf'
        pdf_path = os.path.join(output_dir, pdf_filename)
        doc = SimpleDocTemplate(pdf_path, pagesize=A4)
        styles = getSampleStyleSheet()
        
        # Create a title style
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=styles['Heading1'],
            fontSize=18,
            alignment=1,  # Center alignment
        )
        
        # Create a subtitle style
        subtitle_style = ParagraphStyle(
            'SubtitleStyle',
            parent=styles['Heading2'],
            fontSize=14,
        )
        
        # Initialize the elements list for the PDF
        elements = []
        
        # Add the report title
        elements.append(Paragraph(f"Route Maps Report - {current_date_minus_1}", title_style))
        elements.append(Spacer(1, 0.25*inch))
        
        # Create the legend
        elements.append(Paragraph("Map Legend", subtitle_style))
        elements.append(Spacer(1, 0.1*inch))
        
        # Legend items in a table format
        legend_data = [
            ["Symbol", "Description"],
            ["Blue Line", "Recommended Route"],
            ["Red Line", "Actual Route Taken"],
            ["Numbered Markers (Blue)", "Journey Nodes"],
            ["'C' Marker (Red)", "Charging Station"],
            ["'D' Marker (Green)", "Depot (Dartford)"],
            ["'S' Marker (Purple)", "Actual Route Start"],
            ["'E' Marker (Orange)", "Actual Route End"],
        ]
        
        legend_table = Table(legend_data, colWidths=[2*inch, 3.5*inch])
        legend_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (1, 0), 12),
            ('BACKGROUND', (0, 1), (0, -1), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        
        elements.append(legend_table)
        elements.append(PageBreak())
        elements.append(Spacer(1, 0.5*inch))
        
        # Connect to the database to get route aliases
        conn = get_connection()
        
        # Find all HTML map files
        html_files = glob.glob(os.path.join(output_dir, "journey_map_*.html"))
        
        if not html_files:
            logging.error(f"No map HTML files found in {output_dir}")
            driver.quit()
            if conn:
                conn.close()
            return None
        
        # Process each map
        for html_file in sorted(html_files):
            try:
                # Extract route_id from filename
                route_id_match = re.search(r'journey_map_(.+?)\.html', html_file)
                if route_id_match:
                    route_id = route_id_match.group(1)
                else:
                    route_id = os.path.basename(html_file)
                
                # Get route_alias from database if connection is available
                route_alias = None
                if conn:
                    try:
                        query = f"""
                            SELECT route_alias 
                            FROM public.t_route_plan 
                            WHERE route_id = '{route_id}'
                        """
                        df = pd.read_sql(query, conn)
                        if not df.empty:
                            route_alias = df.iloc[0]['route_alias']
                    except Exception as e:
                        logging.warning(f"Error getting route_alias: {e}")
                
                # Create title with route_id and route_alias
                if route_alias:
                    map_title = f"Route {route_id} ({route_alias})"
                else:
                    map_title = f"Route {route_id}"
                    
                # Load the HTML file
                file_url = f"file:///{os.path.abspath(html_file)}"
                driver.get(file_url)
                
                # Wait for the map to load
                time.sleep(2)
                
                # Take a screenshot
                # Use a safer filename by replacing problematic characters in route_id
                safe_route_id = re.sub(r'[^a-zA-Z0-9_]', '_', route_id)
                screenshot_path = os.path.join(output_dir, f"screenshot_{safe_route_id}.jpg")
                driver.save_screenshot(screenshot_path)
                
                # Verify the screenshot was created and is readable
                if not os.path.exists(screenshot_path):
                    logging.error(f"Screenshot was not created: {screenshot_path}")
                    continue
                
                # Ensure the file is accessible and has content
                if os.path.getsize(screenshot_path) == 0:
                    logging.error(f"Screenshot file is empty: {screenshot_path}")
                    continue
                
                # Add map title to PDF
                elements.append(Paragraph(map_title, subtitle_style))
                elements.append(Spacer(1, 0.1*inch))
                
                # Add screenshot to PDF with additional error handling
                try:
                    img = Image(screenshot_path, width=7*inch, height=5*inch)
                    img.imageWidth = 7*inch * 0.8
                    img.imageHeight = 5*inch * 0.8
                    elements.append(img)
                    if html_file != sorted(html_files)[-1]: # Don't add page break after the last map
                        elements.append(PageBreak())
                    logging.info(f"Added map for {map_title} to PDF")
                except Exception as e:
                    logging.error(f"Failed to add screenshot to PDF: {e}")
                    # If Image creation fails, add a text note instead
                    elements.append(Paragraph(f"[Screenshot failed to load for {map_title}]", styles["Normal"]))
                    continue
                elements.append(Spacer(1, 0.5*inch))
                
            except Exception as e:
                logging.error(f"Error processing map {html_file}: {e}")
        
        # Close the webdriver and database connection
        driver.quit()
        if conn:
            conn.close()
        
        # Build the PDF
        try:
            doc.build(elements)
            logging.info(f"PDF report created: {pdf_path}")
            
            # Only clean up the screenshot files after the PDF is successfully built
            screenshot_files = glob.glob(os.path.join(output_dir, "screenshot_*.jpg"))
            for sf in screenshot_files:
                try:
                    os.remove(sf)
                    logging.debug(f"Removed temporary screenshot: {sf}")
                except Exception as e:
                    logging.warning(f"Could not remove temporary screenshot {sf}: {e}")
                    
            # Only clean up the map files after the PDF is successfully built
            html_files = glob.glob(os.path.join(output_dir, "journey_map_*.html"))
            for html_file in html_files:
                try:
                    os.remove(html_file)
                    logging.debug(f"Removed HTML file: {html_file}")
                except Exception as e:
                    logging.warning(f"Could not remove HTML file {html_file}: {e}")
            return pdf_path
        except Exception as e:
            logging.error(f"Error creating PDF: {e}")
            return None
    
    def create_route_map(route_id, output_dir, current_date_minus_1):
        """Create and save a map for a specific route ID."""
        # Connect to database
        conn = get_connection()
        if not conn:
            logging.error("Failed to connect to the database.")
            return None
            
        try:
            # Part 1: Get vehicle and route info
            vehicle_id, route_alias = get_vehicle_and_route_info(route_id, conn)
            if not vehicle_id or not route_alias:
                logging.warning(f"Skipping map creation for route {route_id} due to missing vehicle info")
                return None
            
            # Initialize telematics_df as empty DataFrame
            telematics_df = pd.DataFrame()
            
            # Only get timing and telematics data if vehicle_id is not 'X'
            if vehicle_id != 'X':
                # Part 2: Get actual route timing
                route_start_time, route_end_time = get_route_timing(current_date_minus_1, vehicle_id, route_alias, conn)
                
                # Only get telematics data if we have timing
                if route_start_time and route_end_time:
                    # Part 3: Get telematics data
                    temp_df = get_telematics_data(vehicle_id, route_start_time, route_end_time, conn)
                    if temp_df is not None:
                        telematics_df = temp_df
            
            # Part 4: Load the recommendation data 
            # Load journey nodes into GeoDataFrame
            query_nodes = f"""
                SELECT jn.id, jn.x_cord AS lon, jn.y_cord AS lat, jn.geom, jn.node_sequence
                FROM public.t_journey_nodes jn
                JOIN public.t_journeys j ON jn.journey_id = j.journey_id
                WHERE j.route_plan_route_id = '{route_id}'
            """
            gdf_nodes = gpd.read_postgis(query_nodes, conn, geom_col="geom", crs="EPSG:4326")
            gdf_nodes.dropna(inplace=True)  # Remove rows with missing coordinates
            
            # Load road segments into GeoDataFrame
            query_segments = f"""
                WITH journeys AS (
                    SELECT journey_id, vehicle_id
                    FROM t_journeys
                    WHERE journey_id = (SELECT journey_id FROM t_journeys WHERE route_plan_route_id = '{route_id}')
                ),
                ordered_segments AS (
                    SELECT j.vehicle_id, j.journey_id, l.geom_way as geom
                    FROM journeys j 
                    JOIN public.t_road_segments_per_journey rs ON j.journey_id = rs.journey_id
                    JOIN public.hh_2po_4pgr l ON rs.osm_road_segment_id = l.osm_id
                )
                SELECT geom FROM ordered_segments;
            """
            gdf_segments = gpd.read_postgis(query_segments, conn, geom_col="geom")
            gdf_segments.dropna(inplace=True)
            
            # Load charging stations into GeoDataFrame
            query_charging_stations = f"""
                SELECT s.latitude AS lat, s.longitude AS lon, s.geom
                FROM public.t_journey_nodes jn
                JOIN public.t_ev_charging_stations s ON jn.ev_charge_station_id = s.id
                WHERE jn.journey_id = (SELECT journey_id FROM t_journeys WHERE route_plan_route_id = '{route_id}')
                AND jn.node_type = 'CHARGE';
            """
            gdf_charging = gpd.read_postgis(query_charging_stations, conn, geom_col="geom", crs="EPSG:4326")
            gdf_charging.dropna(inplace=True)
            
            # Create folium map centered on the first journey node
            if not gdf_nodes.empty:
                first_node = gdf_nodes.iloc[0]
                m = folium.Map(location=[first_node.lat, first_node.lon], zoom_start=12, tiles="CartoDB positron",)
            else:
                m = folium.Map(location=[51.5, -0.1], zoom_start=12, tiles="CartoDB positron",)  # Default to London
            
            # Add journey nodes
            for _, row in gdf_nodes.iterrows():
                # Create a custom icon with the node sequence number
                icon_html = f'''
                    <div style="
                        background-color: blue;
                        color: white;
                        border-radius: 50%;
                        text-align: center;
                        line-height: 24px;
                        width: 24px;
                        height: 24px;
                        font-weight: bold;
                        font-size: 14px;">
                        {round(row.node_sequence)}
                    </div>
                '''
                folium.Marker(
                    [row.lat, row.lon],
                    popup=f"Node {row.node_sequence}",
                    icon=folium.DivIcon(html=icon_html, icon_size=(24, 24))
                    ).add_to(m)
            
            # Add road segments with direction arrows and information popups
            segment_counter = 0
            for idx, row in gdf_segments.iterrows():
                if isinstance(row.geom, LineString):
                    coords = list(row.geom.coords)
                    line_points = [(pt[1], pt[0]) for pt in coords]
                    
                    # Add the polyline with popup
                    line = folium.PolyLine(
                        line_points,
                        color='blue',
                        weight=3,
                    ).add_to(m)
                    
                    # Add arrow symbols only on every 10th line
                    segment_counter += 1
                    if segment_counter % 20 == 0:
                        PolyLineTextPath(
                            line,
                            text="→",
                            repeat=False,
                            offset=8,
                            attributes={"fill": "#0000FF", "font-weight": "bold", "font-size": "14"}
                        ).add_to(m)
            
            # Add charging stations with 'C' icons
            for _, row in gdf_charging.iterrows():
                # Create a custom icon for charging stations
                charge_icon_html = '''
                    <div style="
                        background-color: red;
                        color: white;
                        border-radius: 50%;
                        text-align: center;
                        line-height: 24px;
                        width: 24px;
                        height: 24px;
                        font-weight: bold;
                        font-size: 14px;">
                        C
                    </div>
                '''
                
                folium.Marker(
                    [row.lat, row.lon],
                    icon=folium.DivIcon(html=charge_icon_html, icon_size=(24, 24))
                ).add_to(m)
            
            # Depot marker for Dartford
            depot_lat = 51.463121
            depot_lon = 0.246687
            
            # Depot icon
            depot_icon_html = '''
                    <div style="
                        background-color: green;
                        color: white;
                        border-radius: 50%;
                        text-align: center;
                        line-height: 24px;
                        width: 24px;
                        height: 24px;
                        font-weight: bold;
                        font-size: 14px;">
                        D
                    </div>
                '''

            # Add depot marker
            folium.Marker(
                    [depot_lat, depot_lon],
                    popup="Dartford Depot",
                    icon=folium.DivIcon(html=depot_icon_html, icon_size=(24, 24))
                ).add_to(m)
            
            # Part 5: Plot actual journey from telematics data (after plotting recommended route)
            # Convert telematics data to line
            if not telematics_df.empty:
                # Create a line from the telematics points
                actual_route_points = [(row['latitude'], row['longitude']) for _, row in telematics_df.iterrows()]
                
                # Add the actual route as a polyline in a different color (red)
                folium.PolyLine(
                    actual_route_points,
                    color='red',
                    weight=3,
                    opacity=0.7,
                    popup="Actual Route"
                ).add_to(m)
                
                # Add markers for start and end of actual route
                start_point = actual_route_points[0]
                end_point = actual_route_points[-1]
                
                # Start marker (green S)
                start_icon_html = '''
                    <div style="
                        background-color: purple;
                        color: white;
                        border-radius: 50%;
                        text-align: center;
                        line-height: 24px;
                        width: 24px;
                        height: 24px;
                        font-weight: bold;
                        font-size: 14px;">
                        S
                    </div>
                '''
                folium.Marker(
                    start_point,
                    popup=f"Actual Start: {route_start_time}",
                    icon=folium.DivIcon(html=start_icon_html, icon_size=(24, 24))
                ).add_to(m)
                
                # End marker (red E)
                end_icon_html = '''
                    <div style="
                        background-color: orange;
                        color: white;
                        border-radius: 50%;
                        text-align: center;
                        line-height: 24px;
                        width: 24px;
                        height: 24px;
                        font-weight: bold;
                        font-size: 14px;">
                        E
                    </div>
                '''
                folium.Marker(
                    end_point,
                    popup=f"Actual End: {route_end_time}",
                    icon=folium.DivIcon(html=end_icon_html, icon_size=(24, 24))
                ).add_to(m)
            
            # Find extreme points and calculate center
            all_points = []

            # Add all journey nodes
            for _, row in gdf_nodes.iterrows():
                all_points.append((row.lon, row.lat))
                
            # Add all charging stations
            for _, row in gdf_charging.iterrows():
                all_points.append((row.lon, row.lat))
            
            # Add the depot
            all_points.append((depot_lon, depot_lat))
                
            # Add actual route points from telematics
            if not telematics_df.empty:
                for _, row in telematics_df.iterrows():
                    all_points.append((row['longitude'], row['latitude']))

            # Find extremes (if there are any points)
            if all_points:
                north = max(all_points, key=lambda p: p[1])[1]
                south = min(all_points, key=lambda p: p[1])[1]
                east = max(all_points, key=lambda p: p[0])[0]
                west = min(all_points, key=lambda p: p[0])[0]
                
                # Adjust map center and zoom to fit all points
                m.fit_bounds([[south, west], [north, east]])
            
            Fullscreen().add_to(m)
            
            # Create output directory if it doesn't exist
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Save map with route_id in filename
            map_filename = os.path.join(output_dir, f"journey_map_{route_id}.html")
            m.save(map_filename)
            logging.info(f"Map saved as {map_filename}")
            
            return map_filename
        except Exception as e:
            logging.error(f"Error creating map: {e}")
            return None
        finally:
            # Close connection
            conn.close()
    
    def upload_to_blob_storage(file_path):
        """
        Uploads a file to Azure Blob Storage.
        
        Args:
            file_path (str): Path to the file to upload
        Returns:
            str: URL of the uploaded blob if successful, None otherwise
        """
        try:
            # Check if the file exists
            if not os.path.exists(file_path):
                logging.error(f'File not found: {file_path}')
                return None
            
            # Get connection string from environment variables
            connection_string = os.getenv('storage_account_conn_string')
            if not connection_string:
                logging.error("Azure Storage connection string not found in environment variables")
                return None
            
            # Create the BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Get or create the container
            container_name = os.getenv('blob_container_name')
            if not container_name:
                logging.error('Blob container does not exist')
                return None
            
            try:
                container_client = blob_service_client.get_container_client(container_name)
                # Check if container exists, if not create it
                if not container_client.exists():
                    container_client = blob_service_client.create_container(container_name)
                    logging.info(f"Created container: {container_name}")
            except Exception as e:
                logging.error(f"Error accessing or creating container: {e}")
                return None
                
            # Get the file name from the path
            file_name = os.path.basename(file_path)
            
            # Upload the file
            blob_dir_path = os.getenv('blob_dir_path')
            blob_path = f'{blob_dir_path}/{file_name}'
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type='application/pdf'))
            
            logging.info(f"Uploaded {file_name} to {container_name} container")
            
            # Get the URL of the uploaded blob
            blob_url = blob_client.url
            
            return blob_url
            
        except Exception as e:
            logging.error(f"Error uploading file to blob storage: {e}")
            return None
    
    """Main function to process routes from CSV and create maps."""
    # Define CSV file path and output directory
    site_id = os.getenv('site_id', 10)
    # Determine the temporary directory based on the OS
    temp_directory = "/tmp" if os.name == "posix" else "D:\\local\\Temp"
    current_date_minus_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # Load routes
    try:
        route_numbers = fetch_route_numbers(site_id)
        # Check if route numbers exist
        if not route_numbers:
            logging.error(f"No routes found.")
            return
        logging.info(f'Fetched routes: {route_numbers}')
        routes_df = pd.DataFrame(route_numbers, columns=['route_no'])
        # Create maps for each route
        for route_id in routes_df['route_no'].unique():
            logging.info(f"Processing route: {route_id}")
            create_route_map(route_id, temp_directory, current_date_minus_1)
        
        logging.info(f"All maps saved to {temp_directory}")
        
        # Create PDF report with all maps
        logging.info("Creating PDF report with screenshots...")
        pdf_path = create_pdf_report(temp_directory, current_date_minus_1)
        if pdf_path:
            logging.info(f"PDF report created: {pdf_path}")
            
            # Upload to Azure Blob storage
            blob_url = upload_to_blob_storage(pdf_path)
            if blob_url:
                logging.info(f'PDF uploaded to blob storage: {blob_url}')
            else:
                logging.error('Failed to upload PDF to blob storage')
        
    except Exception as e:
        logging.error(f"Error processing routes: {e}")