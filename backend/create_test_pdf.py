# Create a simple test PDF or text file
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    
    c = canvas.Canvas('../data/test_sample.pdf', pagesize=letter)
    c.drawString(100, 750, 'Test PDF Document')
    c.drawString(100, 720, 'Name: John Doe')
    c.drawString(100, 690, 'Age: 30')
    c.drawString(100, 660, 'City: New York')
    c.drawString(100, 630, 'Email: john@example.com')
    c.drawString(100, 600, 'Phone: 555-1234')
    c.save()
    print('✅ Created test PDF')
except ImportError:
    print('⚠️ reportlab not available, creating text file instead')
    with open('../data/test_sample.txt', 'w') as f:
        f.write('Test Document\nName: John Doe\nAge: 30\nCity: New York\nEmail: john@example.com\nPhone: 555-1234')
    print('✅ Created test text file')
except Exception as e:
    print(f'❌ Error: {e}')