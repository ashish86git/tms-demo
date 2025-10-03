# --------------------------------------------------------------------------------------
# This is a full-featured Flask application for a Transport Management System (TMS).
# It includes routes for authentication, fleet management, driver management,
# indent processing, and a financial dashboard.
# The code has been refactored to ensure correct database interaction using psycopg2.
# --------------------------------------------------------------------------------------

from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash, jsonify, make_response, g
import pandas as pd
import os
import io
from decimal import Decimal
from collections import defaultdict
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from psycopg2.extras import RealDictCursor
import psycopg2
from datetime import datetime, date, timedelta

# -------------------------- Configuration and Initialization --------------------------

UPLOAD_FOLDER = os.path.join('static', 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = 'tms-secret-key'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Database configuration
db_config = {
    'host': 'c7s7ncbk19n97r.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com',
    'user': 'u7tqojjihbpn7s',
    'password': 'p1b1897f6356bab4e52b727ee100290a84e4bf71d02e064e90c2c705bfd26f4a5',
    'database': 'd8lp4hr6fmvb9m',
    'port': 5432
}


def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        dbname=db_config['database'],
        port=db_config['port']
    )
    return conn

# Database table creation function
def create_tables():
    """Creates the necessary tables if they do not already exist."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Create driver_master table (if it doesn't exist)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS driver_master (
                driver_id VARCHAR(50) PRIMARY KEY,
                driver_name VARCHAR(100) NOT NULL,
                license_number VARCHAR(50) NOT NULL,
                contact_number VARCHAR(20),
                address TEXT,
                availability VARCHAR(20),
                shift_info VARCHAR(50),
                aadhar_file VARCHAR(255),
                license_file VARCHAR(255)
            );
        """)

        # Create driver_financials table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS driver_financials (
                financial_id SERIAL PRIMARY KEY,
                driver_id VARCHAR(50) REFERENCES driver_master(driver_id) ON DELETE CASCADE,
                salary NUMERIC(10, 2) NOT NULL,
                bonus NUMERIC(10, 2) DEFAULT 0.00,
                last_paid_date DATE
            );
        """)

        conn.commit()
        print("Tables created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn:
            conn.close()

# Create tables when the application starts
with app.app_context():
    create_tables()


# -------------------------- Authentication Routes --------------------------

@app.route('/', methods=['GET', 'POST'])
def auth():
    """Handles user login and signup."""
    if request.method == 'POST':
        form_type = request.form.get('form_type')

        if form_type == 'login':
            username = request.form['username']
            password = request.form['password']

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM users_tms WHERE username = %s', (username,))
            user = cursor.fetchone()
            cursor.close()
            conn.close()

            if user and (user['password'] == password or check_password_hash(user['password'], password)):
                session['user'] = username
                return redirect(url_for('dashboard'))

            return render_template('login.html', error='Invalid username or password', form_type='login')

        elif form_type == 'signup':
            username = request.form['username']
            email = request.form['email']
            password = request.form['password']

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM users_tms WHERE username = %s', (username,))
            existing_user = cursor.fetchone()

            if existing_user:
                cursor.close()
                conn.close()
                return render_template('login.html', error='Username already exists', form_type='signup')
            else:
                hashed_password = generate_password_hash(password)
                cursor.execute(
                    'INSERT INTO users_tms (username, email, password) VALUES (%s, %s, %s)',
                    (username, email, hashed_password)
                )
                conn.commit()
                cursor.close()
                conn.close()
                session['user'] = username
                return redirect(url_for('dashboard'))

    return render_template('login.html', form_type='login')


@app.route('/dashboard')
def dashboard():
    """Renders the main dashboard page."""
    if 'user' not in session:
        return redirect(url_for('auth'))
    return render_template('dashboard.html', username=session['user'])


@app.route('/logout')
def logout():
    """Logs the user out by clearing the session."""
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('auth'))


# -------------------------- Fleet Master Routes --------------------------

@app.route('/fleet_master', methods=['GET'])
def fleet_master():
    """Displays the fleet master data and handles filtering."""
    if 'user' not in session:
        # A temporary fix for demo, should be handled by login redirect.
        # Original code had this, so keeping it for the requested flow.
        session['user'] = 'Admin'

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM fleet ORDER BY vehicle_id")
    rows = cursor.fetchall()

    fleet_data = [{
        'vehicle_id': row[0],
        'vehicle_name': row[1],
        'make': row[2],
        'model': row[3],
        'vin': row[4],
        'type': row[5],
        'group': row[6],
        'status': row[7],
        'license_plate': row[8],
        'current_meter': row[9],
        'capacity_wei': row[10],
        'capacity_vol': row[11],
        'documents_expiry': row[12].strftime('%Y-%m-%d') if row[12] else '',
        'driver_id': row[13],
        'date_of_join': row[14].strftime('%Y-%m-%d') if row[14] else '',
        'avg': row[15] if row[15] is not None else 0
    } for row in rows]

    cursor.close()
    conn.close()

    return render_template('fleet_master.html', data=fleet_data, user=session['user'])


@app.route('/fleet_master/add', methods=['POST'])
def add_vehicle():
    """Adds a new vehicle to the fleet database."""
    form = request.form

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO fleet (
                vehicle_id, vehicle_name, make, model, vin, type, "group", status,
                license_plate, current_meter, capacity_weight_kg, capacity_vol_cbm,
                documents_expiry, driver_id, date_of_join, avg
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            form['vehicle_id'], form['vehicle_name'], form['make'], form['model'],
            form['vin'], form['type'], form['group'], form['status'],
            form['license_plate'], int(form['current_meter']),
            float(form['capacity_wei']), float(form['capacity_vol']),
            datetime.strptime(form['documents_expiry'], '%Y-%m-%d'),
            form['driver_id'],
            datetime.strptime(form['date_of_join'], '%Y-%m-%d'),
            float(form.get('avg') or 0)
        ))

        conn.commit()
        flash('Vehicle added successfully!', 'success')
    except psycopg2.IntegrityError:
        conn.rollback()
        flash('Vehicle ID already exists.', 'danger')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()

    return redirect('/fleet_master')


@app.route('/fleet_master/edit/<vehicle_id>', methods=['GET', 'POST'])
def edit_vehicle(vehicle_id):
    """Edits an existing vehicle's details."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == 'POST':
        form = request.form
        try:
            documents_expiry = form.get('documents_expiry')
            documents_expiry = datetime.strptime(documents_expiry, '%Y-%m-%d').date() if documents_expiry else None
            date_of_join = form.get('date_of_join')
            date_of_join = datetime.strptime(date_of_join, '%Y-%m-%d').date() if date_of_join else None

            cursor.execute("""
                UPDATE fleet
                SET vehicle_name = %s, driver_id = %s, make = %s, model = %s, vin = %s,
                    type = %s, "group" = %s, status = %s, license_plate = %s,
                    current_meter = %s, capacity_weight_kg = %s, capacity_vol_cbm = %s,
                    documents_expiry = %s, date_of_join = %s, avg = %s
                WHERE vehicle_id = %s
            """, (
                form.get('vehicle_name'), form.get('assigned_driver'), form.get('make'),
                form.get('model'), form.get('vin'), form.get('type'), form.get('group'),
                form.get('status'), form.get('license_plate'), int(form.get('current_meter') or 0),
                float(form.get('capacity_weight_kg') or 0), float(form.get('capacity_vol_cbm') or 0),
                documents_expiry, date_of_join, float(form.get('avg') or 0), vehicle_id
            ))

            conn.commit()
            flash('Vehicle updated successfully!', 'success')
            return redirect('/fleet_master')

        except Exception as e:
            conn.rollback()
            flash(f'Error updating vehicle: {str(e)}', 'danger')
            return redirect('/fleet_master')
        finally:
            cursor.close()
            conn.close()

    # GET method
    cursor.execute("SELECT * FROM fleet WHERE vehicle_id = %s", (vehicle_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        flash('Vehicle not found.', 'warning')
        return redirect('/fleet_master')

    vehicle_data = {
        'vehicle_id': row[0], 'vehicle_name': row[1], 'make': row[2],
        'model': row[3], 'vin': row[4], 'type': row[5], 'group': row[6],
        'status': row[7], 'license_plate': row[8], 'current_meter': row[9],
        'capacity_weight_kg': row[10], 'capacity_vol_cbm': row[11],
        'documents_expiry': row[12].strftime('%Y-%m-%d') if row[12] else '',
        'driver_id': row[13],
        'date_of_join': row[14].strftime('%Y-%m-%d') if row[14] else '',
        'avg': row[15] if row[15] is not None else 0
    }

    return render_template('edit_vehicle.html', vehicle=vehicle_data, user=session.get('user', ''))


# -------------------------- Driver Master Routes --------------------------

@app.route('/driver_master', methods=['GET', 'POST'])
def driver_master():
    """Manages driver data, including adding new drivers and displaying the list."""
    if 'user' not in session:
        return redirect('/')

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # --- Fetch fleet vehicle_ids for dropdown ---
    cur.execute("SELECT vehicle_id FROM fleet")
    fleet_rows = cur.fetchall()
    fleet_data = [row['vehicle_id'] for row in fleet_rows]

    if request.method == 'POST':
        form_data = request.form.to_dict()

        # Handle file uploads
        aadhar_file = request.files.get('aadhar_file')
        license_file = request.files.get('license_file')

        aadhar_filename = secure_filename(aadhar_file.filename) if aadhar_file and aadhar_file.filename else None
        license_filename = secure_filename(license_file.filename) if license_file and license_file.filename else None

        if aadhar_filename:
            aadhar_path = os.path.join(app.config['UPLOAD_FOLDER'], aadhar_filename)
            aadhar_file.save(aadhar_path)
        if license_filename:
            license_path = os.path.join(app.config['UPLOAD_FOLDER'], license_filename)
            license_file.save(license_path)

        try:
            # --- Insert into driver_master table including vehicle_id ---
            cur.execute("""
                INSERT INTO driver_master (
                    driver_id, driver_name, license_number, contact_number,
                    address, availability, shift_info, vehicle_id, aadhar_file, license_file
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                form_data['driver_id'], form_data['driver_name'], form_data['license_number'],
                form_data['contact_number'], form_data['address'], form_data['availability'],
                form_data['shift_info'], form_data['vehicle_id'], aadhar_filename, license_filename
            ))

            # Insert into driver_financials table (salary)
            salary = Decimal(form_data.get('salary', 0))
            cur.execute("""
                INSERT INTO driver_financials (driver_id, salary)
                VALUES (%s, %s)
            """, (form_data['driver_id'], salary))

            conn.commit()
            flash('Driver added successfully!', 'success')
        except psycopg2.IntegrityError:
            conn.rollback()
            flash('Driver ID already exists or a foreign key constraint failed.', 'danger')
        except Exception as e:
            conn.rollback()
            flash(f'An error occurred: {str(e)}', 'danger')

        return redirect(url_for('driver_master'))

    # Fetch all drivers using LEFT JOIN to include financial data
    cur.execute("""
        SELECT
            dm.driver_id,
            dm.driver_name,
            dm.license_number,
            dm.contact_number,
            dm.address,
            dm.availability,
            dm.shift_info,
            dm.vehicle_id,
            dm.aadhar_file,
            dm.license_file,
            df.salary
        FROM driver_master AS dm
        LEFT JOIN driver_financials AS df ON dm.driver_id = df.driver_id;
    """)

    data = cur.fetchall()
    cur.close()
    conn.close()

    return render_template('driver_master.html', data=data, fleet_data=fleet_data)





# -------------------------- Unused/Placeholder Routes --------------------------
# The following routes were in the original code but were incomplete or not
# connected to a database. They are kept here to maintain the original file
# structure but should be reviewed and implemented properly.

vehicles = []
service_records = []
vehicle_counter = 1
service_counter = 1


@app.route('/vehicle_maintenance')
def vehicle_maintenance():
    if 'user' not in session:
        return redirect('/')
    filters = {'vehicle_id': request.args.get('vehicle_id', '').strip(),
               'assigned_driver': request.args.get('assigned_driver', '').strip(),
               'status': request.args.get('status', '').strip()}
    filtered = vehicles
    if filters['vehicle_id']: filtered = [v for v in filtered if
                                          filters['vehicle_id'].lower() in v['vehicle_id'].lower()]
    if filters['assigned_driver']: filtered = [v for v in filtered if
                                               filters['assigned_driver'].lower() in v['assigned_driver'].lower()]
    if filters['status']: filtered = [v for v in filtered if v['status'] == filters['status']]
    return render_template('vehicle_maintenance.html', vehicles=filtered, filters=filters)


@app.route('/add_vehicle', methods=['GET', 'POST'])
def add_vehicle_form():
    global vehicle_counter
    if request.method == 'POST':
        data = request.form.to_dict()
        data['id'] = vehicle_counter
        data['service_cost'] = float(data.get('service_cost') or 0)
        data['last_service_date'] = datetime.strptime(data.get('last_service_date', ''), '%Y-%m-%d') if data.get(
            'last_service_date') else None
        data['next_service_due'] = datetime.strptime(data.get('next_service_due', ''), '%Y-%m-%d') if data.get(
            'next_service_due') else None
        vehicles.append(data)
        vehicle_counter += 1
        flash("Vehicle added successfully", "success")
        return redirect(url_for('vehicle_maintenance'))
    return render_template('add_vehicle.html')


@app.route('/add_service/<int:vehicle_id>', methods=['GET', 'POST'])
def add_service(vehicle_id):
    global service_counter
    vehicle = next((v for v in vehicles if v['id'] == vehicle_id), None)
    if not vehicle:
        flash("Vehicle not found", "danger")
        return redirect(url_for('vehicle_maintenance'))
    if request.method == 'POST':
        service = request.form.to_dict()
        service['id'] = service_counter
        service['vehicle_id'] = vehicle_id
        service['service_date'] = datetime.strptime(service.get('service_date'), '%Y-%m-%d')
        service['next_service_due'] = datetime.strptime(service.get('next_service_due'), '%Y-%m-%d')
        service['service_cost'] = float(service.get('service_cost') or 0)
        service_records.append(service)
        service_counter += 1
        vehicle['last_service_date'] = service['service_date']
        vehicle['next_service_due'] = service['next_service_due']
        vehicle['service_type'] = service.get('service_type')
        vehicle['status'] = service.get('status')
        vehicle['parts_replaced'] = service.get('parts_replaced')
        vehicle['service_cost'] = service['service_cost']
        vehicle['notes'] = service.get('notes')
        flash("Service added successfully", "success")
        return redirect(url_for('vehicle_maintenance'))
    return render_template('add_service.html', vehicle=vehicle)


@app.route('/delete_vehicle_men/<int:vehicle_id>', methods=['POST'])
def delete_vehicle_men(vehicle_id):
    global vehicles
    vehicles = [v for v in vehicles if v['id'] != vehicle_id]
    flash("Vehicle deleted successfully", "success")
    return redirect(url_for('vehicle_maintenance'))


tyres = []


@app.route('/tyre-management', methods=['GET', 'POST'])
def tyre_management():
    if 'user' not in session:
        return redirect('/')
    if request.method == 'POST':
        serial_number = request.form.get('serial_number')
        vehicle_id = request.form.get('vehicle_id')
        position = request.form.get('position')
        status = request.form.get('status')
        installed_on = request.form.get('installed_on')
        km_run = request.form.get('km_run')
        last_inspection = request.form.get('last_inspection')
        condition = request.form.get('condition')
        installed_on = datetime.strptime(installed_on, '%Y-%m-%d')
        last_inspection = datetime.strptime(last_inspection, '%Y-%m-%d')
        tyres.append({'serial_number': serial_number, 'vehicle_id': vehicle_id, 'position': position, 'status': status,
                      'installed_on': installed_on, 'km_run': int(km_run), 'last_inspection': last_inspection,
                      'condition': condition})
        flash('Tyre added successfully!', 'success')
        return redirect('/tyre-management')
    return render_template('tyre_management.html', tyres=tyres)


@app.route('/download_report')
def download_report():
    path = os.path.join('data/', 'trip_logs.csv')
    return send_file(path, as_attachment=True)

# --------- ORDER MANAGEMENT -----------
orders_data = []
@app.route('/orders', methods=['GET', 'POST'])
def orders():
    if 'user' not in session:
        return redirect('/')

    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == 'POST':
        data = request.form
        order_id = data['order_id']

        # Check if order exists
        cur.execute("SELECT 1 FROM orders WHERE order_id = %s", (order_id,))
        exists = cur.fetchone()

        if exists:
            # Update existing order
            cur.execute("""
                UPDATE orders SET
                    customer_name = %s,
                    created_date  = %s,
                    order_type = %s,
                    pickup_location_latlon = %s,
                    drop_location_latlon = %s,
                    volume_cbm = %s,
                    weight_kg = %s,
                    delivery_priority = %s,
                    expected_delivery = %s,
                    amount = %s,
                    status = %s
                WHERE order_id = %s
            """, (
                data['customer_name'],
                data['created_date'],
                data['order_type'],
                data['pickup_location_latlon'],
                data['drop_location_latlon'],
                data['volume_cbm'],
                data['weight_kg'],
                data['delivery_priority'],
                data['expected_delivery'],
                data['amount'],
                data['status'],
                order_id
            ))
        else:
            # Insert new order
            cur.execute("""
                INSERT INTO orders (
                    order_id, customer_name, created_date, order_type, pickup_location_latlon,
                    drop_location_latlon, volume_cbm, weight_kg,
                    delivery_priority, expected_delivery, amount, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['order_id'],
                data['customer_name'],
                data['created_date'],
                data['order_type'],
                data['pickup_location_latlon'],
                data['drop_location_latlon'],
                data['volume_cbm'],
                data['weight_kg'],
                data['delivery_priority'],
                data['expected_delivery'],
                data['amount'],
                data['status']
            ))

        conn.commit()

    # Fetch all orders
    cur.execute("SELECT * FROM orders ORDER BY expected_delivery")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    data = [dict(zip(colnames, row)) for row in rows]

    cur.close()
    conn.close()

    return render_template('orders.html', data=data)


@app.route('/delete_order/<order_id>', methods=['POST'])
def delete_order(order_id):
    if 'user' not in session:
        return redirect('/')

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error deleting order:", e)
    finally:
        cur.close()
        conn.close()

    return redirect('/orders')
@app.route('/upload_orders', methods=['POST'])
def upload_orders():
    if 'user' not in session:
        return redirect('/')

    file = request.files['orders_file']
    if file and file.filename.endswith('.csv'):
        import pandas as pd
        df = pd.read_csv(file)
        conn = get_db_connection()
        cur = conn.cursor()

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO orders (
                    order_id, customer_name, created_date, order_type, pickup_location_latlon,
                    drop_location_latlon, volume_cbm, weight_kg,
                    delivery_priority, expected_delivery, amount, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    created_date = EXCLUDED.created_date,
                    order_type = EXCLUDED.order_type,
                    pickup_location_latlon = EXCLUDED.pickup_location_latlon,
                    drop_location_latlon = EXCLUDED.drop_location_latlon,
                    volume_cbm = EXCLUDED.volume_cbm,
                    weight_kg = EXCLUDED.weight_kg,
                    delivery_priority = EXCLUDED.delivery_priority,
                    expected_delivery = EXCLUDED.expected_delivery,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status
            """, (
                row['Order_ID'], row['Customer_Name'], row['created_date'], row['Order_Type'],
                row['Pickup_Location_LatLon'], row['Drop_Location_LatLon'],
                row['Volume_CBM'], row['Weight_KG'],
                row['Delivery_Priority'], row['Expected_Delivery'], row['amount'],
                row['Status']
            ))

        conn.commit()
        cur.close()
        conn.close()

    return redirect('/orders')

# ---------------- EDIT (Pre-fill Form) ----------------
@app.route('/edit_order/<order_id>')
def edit_order(order_id):
    if 'user' not in session:
        return redirect('/')

    order = next((o for o in orders_data if o['Order_ID'] == order_id), None)
    if not order:
        return redirect('/orders')
    return render_template('orders.html', data=orders_data, edit_order=order)





@app.route('/optimize')
def optimize():


    return render_template('route_optimize.html')

@app.route('/trip-history')
def trip_history():


    return render_template('trip_history.html')


@app.route('/tracking')
def tracking():
    return render_template('tracking.html')

@app.route('/driver_handover')
def driver_handover():
    return render_template('driver_handover.html')

if __name__ == '__main__':
    app.run(debug=True)
