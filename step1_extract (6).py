import os
import sys
import argparse
from language.processors import load_spacy_model
from utils.html_extractor import extract_translatable_html
from utils.output_generator import generate_output_files

SPACY_MODELS = {
    "en": "en_core_web_sm",
    "zh": "zh_core_web_sm",
    "fr": "fr_core_news_sm",
    "es": "es_core_news_sm",
    "de": "de_core_news_sm",
    "it": "it_core_news_sm",
    "pt": "pt_core_news_sm",
    "ru": "ru_core_news_sm",
    "el": "el_core_news_sm",
    "ca": "es_core_news_sm",
    "eu": "xx_ent_wiki_sm",
    "xx": "xx_ent_wiki_sm"
}

def main():
    SUPPORTED_LANGS = ", ".join(sorted(SPACY_MODELS.keys()))

    parser = argparse.ArgumentParser(
        description="Extract translatable text from HTML, SQL, or Python files.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Change input_file to accept multiple files
    parser.add_argument("input_files", nargs='+', help="Path(s) to the file(s) to process")
    parser.add_argument("--output-dir", default=".", help="Output directory for extracted files")
    parser.add_argument("--file-type", choices=["html", "sql", "python"], default="html", 
                        help="Type of file to process")
    parser.add_argument("--lang", choices=SPACY_MODELS.keys(), required=True, 
                        help=f"Primary language code: {SUPPORTED_LANGS}")
    parser.add_argument("--secondary-lang", choices=SPACY_MODELS.keys(), 
                        help="Optional secondary language code")

    args = parser.parse_args()

    if args.secondary_lang and args.secondary_lang == args.lang:
        parser.error("Primary and secondary languages cannot be the same!")

    # Process each input file
    for input_file in args.input_files:
        # Auto-detect file type if not specified
        if args.file_type == "html" and input_file.endswith(('.py', '.pyw', '.jinja', '.jinja2', '.j2')):
            actual_file_type = "python"
            if input_file.endswith(('.jinja', '.jinja2', '.j2')):
                print(f"Auto-detected Jinja2 template file: {input_file}")
            else:
                print(f"Auto-detected Python file: {input_file}")
        elif args.file_type == "html" and input_file.endswith('.sql'):
            actual_file_type = "sql"
            print(f"Auto-detected SQL file: {input_file}")
        else:
            actual_file_type = args.file_type
        
        # Process based on file type
        if actual_file_type == "html":
            structured, flattened, soup = extract_translatable_html(
                input_file,
                args.lang,
                args.secondary_lang,
                args.output_dir
            )
            generate_output_files(structured, flattened, soup, args.output_dir)
            
        elif actual_file_type == "sql":
            from extractors.sql_extractor import extract_translatable_sql
            extract_translatable_sql(
                input_file,
                args.lang,
                secondary_lang=args.secondary_lang,
                output_dir=args.output_dir
            )
            
        elif actual_file_type == "python":
            from extractors.python_extractor import extract_translatable_python
            extract_translatable_python(
                input_file,
                args.lang,
                secondary_lang=args.secondary_lang,
                output_dir=args.output_dir
            )

        # Determine output file extension
        if actual_file_type == "html":
            output_ext = "html"
        elif actual_file_type == "sql":
            output_ext = "sql"
        elif actual_file_type == "python":
            output_ext = "py"
        
        print(f"✅ Processed {input_file}: saved translatable_flat.json, translatable_structured.json, " +
              f"translatable_flat_sentences.json, and non_translatable.{output_ext} in {args.output_dir}")

if __name__ == "__main__":
    main()