# routes/data_routes.py

from flask import render_template, request, flash, redirect, url_for
from pathlib import Path
import os

# Define the path to the raw data directory
# Assumes this 'routes' folder is in the project root
UPLOAD_FOLDER = Path(__file__).resolve().parent.parent / "data" / "raw"
ALLOWED_EXTENSIONS = {'xlsx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def add_data():
    if request.method == 'POST':
        # Check if the post request has the file parts
        sales_file = request.files.get('sales_file')
        credit_file = request.files.get('credit_file')
        margin_file = request.files.get('margin_file')

        files_to_process = {
            '2024.xlsx': sales_file, # The target filename
            'Credit_Balances.xlsx': credit_file,
            'tiles_margin.xlsx': margin_file
        }

        files_uploaded = 0
        for target_filename, file in files_to_process.items():
            # If the user does not select a file, the browser submits an empty file without a filename.
            if file and file.filename != '' and allowed_file(file.filename):
                filepath = os.path.join(UPLOAD_FOLDER, target_filename)
                file.save(filepath)
                files_uploaded += 1
        
        if files_uploaded > 0:
            flash(f'{files_uploaded} file(s) uploaded successfully! Please run the data processing script to update the database.', 'success')
        else:
            flash('No files selected or file types were not allowed.', 'warning')
            
        return redirect(url_for('add_data'))

    # For a GET request, just show the upload page
    return render_template('add_data.html')