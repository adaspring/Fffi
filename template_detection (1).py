# core_scripts/extraction/utils/template_detection.py
"""
Unified template detection for both pasted text and uploaded files.
"""
import re


def detect_template_content(text: str) -> tuple[bool, float, str]:
    """
    Detect if text content contains template syntax.
    
    Args:
        text: The text content to analyze
        
    Returns:
        Tuple of (is_template, confidence_score, template_type)
        - is_template: Boolean indicating if template syntax found
        - confidence_score: Float 0-1 indicating confidence
        - template_type: String like 'jinja2', 'django', 'mixed', or 'none'
    """
    if not text or len(text.strip()) < 10:
        return False, 0.0, 'none'
    
    score = 0.0
    detected_types = []
    
    # Jinja2/Flask template patterns
    jinja_patterns = [
        (r'{%\s*extends\s+["\']', 0.5, 'strong'),  # {% extends "base.html" %}
        (r'{%\s*block\s+\w+', 0.4, 'strong'),      # {% block content %}
        (r'{%\s*include\s+["\']', 0.4, 'strong'),  # {% include "file.html" %}
        (r'{%\s*import\s+', 0.4, 'strong'),        # {% import ... %}
        (r'{%\s*macro\s+\w+', 0.4, 'strong'),      # {% macro name() %}
        (r'{{\s*super\s*\(\s*\)\s*}}', 0.4, 'strong'),  # {{ super() }}
        (r'{{\s*url_for\s*\(', 0.4, 'strong'),     # {{ url_for(...) }}
        (r'{%\s*for\s+\w+\s+in\s+', 0.2, 'medium'), # {% for item in items %}
        (r'{%\s*if\s+', 0.2, 'medium'),            # {% if condition %}
        (r'{%\s*set\s+\w+', 0.2, 'medium'),        # {% set var = value %}
        (r'{{\s*\w+', 0.1, 'weak'),                # {{ variable }}
        (r'{%\s*\w+', 0.1, 'weak'),                # {% tag %}
        (r'{%-?\s*', 0.1, 'weak'),                 # Whitespace control
        (r'\s*-%}', 0.1, 'weak'),                  # Whitespace control
        (r'{#.*?#}', 0.1, 'weak'),                 # {# comment #}
    ]
    
    jinja_score = 0.0
    jinja_strong_found = False
    
    for pattern, weight, strength in jinja_patterns:
        if re.search(pattern, text, re.IGNORECASE | re.MULTILINE):
            jinja_score += weight
            if strength == 'strong':
                jinja_strong_found = True
    
    if jinja_score > 0:
        detected_types.append('jinja2')
        score += min(jinja_score, 0.8)
    
    # Django template patterns
    django_patterns = [
        (r'{%\s*load\s+\w+', 0.5, 'strong'),       # {% load static %}
        (r'{%\s*csrf_token\s*%}', 0.5, 'strong'),  # {% csrf_token %}
        (r'{%\s*static\s+["\']', 0.4, 'strong'),   # {% static "..." %}
        (r'{%\s*url\s+["\']', 0.4, 'strong'),      # {% url "name" %}
        (r'{%\s*trans\s+["\']', 0.4, 'strong'),    # {% trans "..." %}
        (r'{%\s*blocktrans\s*%}', 0.4, 'strong'),  # {% blocktrans %}
        (r'{{\s*block\.super\s*}}', 0.4, 'strong'), # {{ block.super }}
        (r'{%\s*with\s+\w+', 0.2, 'medium'),       # {% with var=value %}
        (r'{%\s*autoescape\s+', 0.2, 'medium'),    # {% autoescape %}
    ]
    
    django_score = 0.0
    django_strong_found = False
    
    for pattern, weight, strength in django_patterns:
        if re.search(pattern, text, re.IGNORECASE | re.MULTILINE):
            django_score += weight
            if strength == 'strong':
                django_strong_found = True
    
    if django_score > 0:
        detected_types.append('django')
        score += min(django_score, 0.8)
    
    # Check for template indicators at the beginning of content
    first_lines = '\n'.join(text.split('\n')[:5])
    if re.search(r'{%\s*(extends|load|import|from)', first_lines):
        score += 0.3
    
    # Check if it looks like a template fragment (no HTML structure)
    has_html_structure = any(tag in text.lower() for tag in ['<!doctype', '<html', '<head>', '<body>'])
    if not has_html_structure and (jinja_score > 0 or django_score > 0):
        score += 0.2
    
    # Determine template type
    if 'jinja2' in detected_types and 'django' in detected_types:
        template_type = 'mixed'
    elif 'jinja2' in detected_types:
        template_type = 'jinja2'
    elif 'django' in detected_types:
        template_type = 'django'
    else:
        template_type = 'none'
    
    # Strong indicators guarantee it's a template
    if jinja_strong_found or django_strong_found:
        score = max(score, 0.8)
    
    is_template = score >= 0.4
    return is_template, min(score, 1.0), template_type


def is_template_file(file_path: str) -> tuple[bool, str]:
    """
    Check if a file contains template syntax.
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        Tuple of (is_template, template_type)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read first 2000 chars to avoid loading huge files
            content = f.read(2000)
        
        is_template, confidence, template_type = detect_template_content(content)
        
        # For files, we can be more confident with lower scores
        if confidence >= 0.3:
            return True, template_type
        
        return False, 'none'
        
    except Exception:
        return False, 'none'