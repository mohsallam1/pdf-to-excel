"""
Streamlit UI for PDF Table Extraction to Excel
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import os
from main import PDFTableExtractor

# Page configuration
st.set_page_config(
    page_title="PDF to Excel Extractor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better Arabic support
st.markdown("""
<style>
    .arabic-text {
        direction: rtl;
        text-align: right;
        font-family: 'Tahoma', 'Arial Unicode MS', sans-serif;
    }
    .main-header {
        text-align: center;
        padding: 2rem 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .stButton>button {
        width: 100%;
        font-size: 1.1rem;
        padding: 0.5rem 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>📊 PDF Table Extractor</h1>
    <p>Extract tables from PDF files and export to Excel with Arabic language support</p>
</div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
    st.subheader("Extraction Options")
    preserve_structure = st.checkbox(
        "Preserve PDF Structure",
        value=True,
        help="Maintain exact table structure from PDF (recommended)"
    )
    
    show_validation = st.checkbox(
        "Show Validation Details",
        value=False,
        help="Display detailed validation metrics"
    )
    
    st.markdown("---")
    st.subheader("📖 Instructions")
    st.markdown("""
    1. Upload your PDF file
    2. Click 'Extract Tables'
    3. Download the Excel file
    4. View extraction summary
    """)
    
    st.markdown("---")
    st.markdown("**Features:**")
    st.markdown("✅ Arabic language support")
    st.markdown("✅ Multiple extraction methods")
    st.markdown("✅ Structure preservation")
    st.markdown("✅ Data validation")

# Main content
uploaded_file = st.file_uploader(
    "📄 Upload PDF File",
    type=['pdf'],
    help="Select a PDF file containing tables to extract"
)

if uploaded_file is not None:
    # Create temporary directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_pdf_path = os.path.join(temp_dir, uploaded_file.name)
        temp_output_dir = os.path.join(temp_dir, "output")
        
        # Save uploaded file
        with open(temp_pdf_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success(f"✅ File uploaded: {uploaded_file.name}")
        
        # Extract button
        if st.button("🔍 Extract Tables", type="primary", use_container_width=True):
            try:
                # Initialize extractor
                with st.spinner("🔄 Processing PDF... This may take a moment."):
                    extractor = PDFTableExtractor(temp_pdf_path, output_dir=temp_output_dir)
                    
                    # Run extraction
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    status_text.text("Starting extraction...")
                    progress_bar.progress(10)
                    
                    df = extractor.extract_and_validate(preserve_structure=preserve_structure)
                    
                    progress_bar.progress(50)
                    status_text.text("Extracting tables...")
                    
                    if not df.empty:
                        progress_bar.progress(70)
                        status_text.text("Exporting to Excel...")
                        
                        # Export to Excel
                        excel_path = extractor.export_to_excel(df)
                        
                        progress_bar.progress(90)
                        status_text.text("Finalizing...")
                        
                        # Read the Excel file for download
                        with open(excel_path, "rb") as excel_file:
                            excel_data = excel_file.read()
                        
                        progress_bar.progress(100)
                        status_text.text("✅ Extraction complete!")
                        
                        st.success("✅ Extraction completed successfully!")
                        
                        # Download button
                        st.download_button(
                            label="📥 Download Excel File",
                            data=excel_data,
                            file_name=f"{Path(uploaded_file.name).stem}_extracted.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                        
                        # Display summary
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Total Rows", len(df))
                        
                        with col2:
                            data_cols = [c for c in df.columns if c is not None and not (isinstance(c, str) and c.startswith('_'))]
                            st.metric("Data Columns", len(data_cols))
                        
                        with col3:
                            if '_confidence' in df.columns:
                                perfect = len(df[df['_confidence'] == 100])
                                st.metric("Perfect Match", perfect)
                            else:
                                st.metric("Perfect Match", len(df))
                        
                        with col4:
                            if '_anomalies' in df.columns:
                                anomalies = len(df[df['_anomalies'] != ''])
                                st.metric("Anomalies", anomalies)
                            else:
                                st.metric("Anomalies", 0)
                        
                        # Show validation details if requested
                        if show_validation and hasattr(extractor, 'metrics'):
                            st.subheader("📊 Extraction Metrics")
                            metrics = extractor.metrics
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write("**Quality Metrics:**")
                                st.write(f"- Perfect Match (100%): {metrics.perfect_match}")
                                st.write(f"- Good Match (90%): {metrics.good_match}")
                                st.write(f"- Flagged (<90%): {metrics.poor_match}")
                            
                            with col2:
                                st.write("**Validation Results:**")
                                st.write(f"- Statistical Outliers: {metrics.statistical_outliers}")
                                st.write(f"- Date Errors: {metrics.chronological_errors}")
                                st.write(f"- Formula Issues: {metrics.formula_inconsistencies}")
                                st.write(f"- Layout Issues: {metrics.layout_issues}")
                                st.write(f"- Duplicates: {metrics.duplicate_rows}")
                        
                        # Display preview of data
                        st.subheader("📋 Data Preview")
                        
                        # Filter out metadata columns for display
                        display_cols = [c for c in df.columns if c is not None and not (isinstance(c, str) and c.startswith('_'))]
                        display_df = df[display_cols].head(20)  # Show first 20 rows
                        
                        if not display_df.empty:
                            st.dataframe(
                                display_df,
                                use_container_width=True,
                                height=400
                            )
                            
                            if len(df) > 20:
                                st.info(f"Showing first 20 rows of {len(df)} total rows. Download the Excel file to see all data.")
                        else:
                            st.warning("No data columns found to display.")
                        
                    else:
                        st.error("❌ No tables found in the PDF. Please check if the PDF contains tables.")
                        progress_bar.empty()
                        status_text.empty()
                
            except Exception as e:
                st.error(f"❌ Error during extraction: {str(e)}")
                st.exception(e)

else:
    # Welcome message
    st.info("👆 Please upload a PDF file to begin extraction.")
    
    # Example section
    with st.expander("ℹ️ About This Tool"):
        st.markdown("""
        ### Features:
        
        - **Multi-Method Extraction**: Uses pdfplumber and Camelot for accurate table extraction
        - **Arabic Language Support**: Properly handles Arabic text with RTL alignment
        - **Structure Preservation**: Maintains exact table layout from PDF
        - **Data Validation**: 11-layer validation system for data quality
        - **Excel Export**: Clean Excel output with proper formatting
        
        ### How It Works:
        
        1. **Upload**: Select your PDF file containing tables
        2. **Extract**: The tool uses multiple methods to extract tables
        3. **Validate**: Data is validated for accuracy and consistency
        4. **Export**: Download the results as an Excel file
        
        ### Supported Formats:
        
        - PDF files with tables
        - Arabic and English text
        - Multi-page documents
        - Complex table structures
        """)

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: gray;'>PDF to Excel Extractor | Built with Streamlit<br>Engineered by Eng: Sallam</div>",
    unsafe_allow_html=True
)

