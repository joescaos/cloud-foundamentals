import firebase_admin
from firebase_admin import credentials, firestore
from flask import Flask, request, jsonify
import pandas as pd
import uuid
import os
from werkzeug.utils import secure_filename

# Flask app setup
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB limit

ALLOWED_EXTENSIONS = {'csv'}

# Create upload folder if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def initialize_firebase():
    # Initialize Firebase Admin SDK
    try: 
        cred = credentials.Certificate('service-account-key.json')
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:  
        print(f"Error initializing Firebase: {e}")
        return None
    
db = initialize_firebase()

def allowed_file(filename):
    """Verifica si el archivo tiene una extensión permitida"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_custom_id():
    """
    Genera un ID similar al formato proporcionado (6QTsClzl06DCdliuFjO7)
    """
    return str(uuid.uuid4().hex[:20])

def validate_persona_data(data):
    """
    Valida que los datos de la persona tengan todos los campos requeridos
    """
    required_fields = [
        'name', 'last_name', 'email', 'age', 'sex', 
        'address', 'country', 'degree', 'university', 'status'
    ]
    
    for field in required_fields:
        if field not in data:
            return False, f"Campo requerido faltante: {field}"
    
    # Valide data types
    try:
        data['age'] = int(data['age'])
        data['status'] = bool(data['status']) if isinstance(data['status'], str) else bool(data['status'])
    except (ValueError, TypeError):
        return False, "Los campos 'age' y 'status' deben ser numéricos/booleanos"
    
    return True, "Datos válidos"

@app.route('/api/persons/upload', methods=['POST'])
def upload_personas_csv():
    """
    Endpoint para cargar personas desde un archivo CSV
    """
    try:
        # Verify the file is in the request
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'message': 'No se encontró el archivo en la solicitud'
            }), 400
        
        file = request.files['file']
        
        # Verify that a file was selected
        if file.filename == '':
            return jsonify({
                'success': False,
                'message': 'No se seleccionó ningún archivo'
            }), 400
        
        # Verify the file type
        if not allowed_file(file.filename):
            return jsonify({
                'success': False,
                'message': 'Tipo de archivo no permitido. Solo se aceptan archivos CSV'
            }), 400
        
        # Save the file temporarily
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        # Read CSV file
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error leyendo el archivo CSV: {str(e)}'
            }), 400
        
        # Convert DataFrame to list of dictionaries
        personas_data = df.to_dict('records')
        
        # Insert data into Firestore
        inserted_count = 0
        errors = []
        
        for index, persona_data in enumerate(personas_data):
            try:
                # Validate data
                is_valid, validation_message = validate_persona_data(persona_data)
                
                if not is_valid:
                    errors.append(f"Fila {index + 2}: {validation_message}")
                    continue
                
                # Generate custom ID
                custom_id = generate_custom_id()
                
                # Insert into Firestore
                db.collection('personas').document(custom_id).set(persona_data)
                inserted_count += 1
                
            except Exception as e:
                errors.append(f"Fila {index + 2}: {str(e)}")
        
        # cleanup - remove the uploaded file
        os.remove(file_path)
        
        response_data = {
            'success': True,
            'message': f'Se insertaron {inserted_count} registros exitosamente',
            'inserted_count': inserted_count,
            'total_records': len(personas_data)
        }
        
        if errors:
            response_data['errors'] = errors
            response_data['message'] = f'Se insertaron {inserted_count} de {len(personas_data)} registros'
        
        return jsonify(response_data), 200 if inserted_count > 0 else 400
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error en el servidor: {str(e)}'
        }), 500


@app.route('/api/persons', methods=['GET'])
def get_persons():
    try:
        limit = request.args.get('limit', default=10, type=int)
        page = request.args.get('page', default=1, type=int)

        if limit > 10:
            limit = 10
        
        persons_ref = db.collection('personas')

        offset = (page - 1) * limit
        
        docs = persons_ref.offset(offset).limit(limit).stream()

        persons_list = []
        for doc in docs:
            person_data = doc.to_dict()
            person_data['id'] = doc.id
            persons_list.append(person_data)

        total_count = len(list(persons_ref.stream()))

        return jsonify({
            'success': True,
            'data': persons_list,
            'pagination': {
                'total': total_count,
                'page': page,
                'limit': limit,
                'pages': (total_count + limit - 1) // limit
            }
        }), 200
    except Exception as e: 
        return jsonify({'success': False, 
                        'message': f'Error getting persons: {str(e)}'
                        }), 500
    
@app.route('/api/persons/<person_id>', methods=['GET'])
def get_person(person_id):
    try:
        doc_ref = db.collection('personas').document(person_id)
        doc = doc_ref.get()
        if doc.exists:
            person_data = doc.to_dict()
            person_data['id'] = doc.id
            return jsonify({'success': True, 'data': person_data}), 200
        else:
            return jsonify({'success': False, 'message': 'Person not found'}), 404
    except Exception as e:  
        return jsonify({'success': False, 
                        'message': f'Error getting person: {str(e)}'
                        }), 500
    
@app.route('/api/persons/<person_id>', methods=['PUT'])
def update_person(person_id):
    try:
        data = request.json
        doc_ref = db.collection('personas').document(person_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.update(data)
            return jsonify({'success': True, 'message': 'Person updated successfully'}), 200
        else:
            return jsonify({'success': False, 'message': 'Person not found'}), 404
    except Exception as e:  
        return jsonify({'success': False, 
                        'message': f'Error updating person: {str(e)}'
                        }), 500
    
@app.route('/api/persons/<person_id>', methods=['DELETE'])
def delete_person(person_id):
    try:
        doc_ref = db.collection('personas').document(person_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.delete()
            return jsonify({'success': True, 'message': 'Person deleted successfully'}), 200
        else:
            return jsonify({'success': False, 'message': 'Person not found'}), 404
    except Exception as e:  
        return jsonify({'success': False, 
                        'message': f'Error deleting person: {str(e)}'
                        }), 500 
    

    

if __name__ == '__main__':
    print("🚀 Iniciando API Flask...")
    print("📝 Endpoints disponibles:")
    print("   POST   /api/personas/upload  - Cargar personas desde CSV")
    print("   GET    /api/personas         - Obtener todas las personas")
    print("   GET    /api/personas/<id>    - Obtener persona por ID")
    print("   PUT    /api/personas/<id>    - Actualizar persona")
    print("   DELETE /api/personas/<id>    - Eliminar persona")
    
    app.run(debug=True, host='0.0.0.0', port=9090)



