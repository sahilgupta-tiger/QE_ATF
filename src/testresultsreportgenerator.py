# COMMAND ----------
from atf.common.atf_cls_pdfformatting import generatePDF
from atf.common.atf_common_functions import log_info,log_error
from atf.common.atf_pdf_constants import *

  # DBTITLE 1,Generate Protocol Summary Report
def generate_protocol_summary_report(df_protocol_summary, protocol_run_details, protocol_run_params, output_path, created_time):
    
    pdfobj_protocol = generatePDF()
    comparison_type = testcasetype + " comparison"
    pdfobj_protocol.write_text(protocolreportheader, 'report header')
    pdfobj_protocol.write_text(protocol_run_details['Test Protocol Name'], 'subheading')
    pdfobj_protocol.write_text(comparison_type , 'subheading')
    pdfobj_protocol.write_text(protocolrunparams, 'section heading')
    pdfobj_protocol.create_table_summary(protocol_run_params)
    pdfobj_protocol.write_text(protocoltestcaseheader, 'section heading')
    pdfobj_protocol.create_table_summary(protocol_run_details)
    pdfobj_protocol.write_text(testresultheader, 'section heading')
    table_type = 'protocol'
    if(testcasetype == "count"):
      table_type = 'protocol_count'
    sno = 1
    test_results_list = ['Failed','Passed']
    for test_result in test_results_list:
      testheader = "3." + str(sno) + ". " + test_result + " Testcases"
      pdfobj_protocol.write_text(testheader, 'section heading')
      if(df_protocol_summary is not None):
        df_protocol_summary_temp = df_protocol_summary.select('*').filter(col('Test Result') == test_result)
        if(df_protocol_summary_temp.count() == 0):
          df_protocol_summary_temp = None
      else:
        df_protocol_summary_temp = None

      pdfobj_protocol.create_table_details(df_protocol_summary_temp, table_type)
      sno = sno + 1
  
    protocol_output_path = output_path + "/run_" + protocol_run_details['Test Protocol Name'] + "_" + created_time+".pdf"
    pdfobj_protocol.pdf.output(protocol_output_path, 'F')
    log_info("Protocol Summary PDF Generated")
