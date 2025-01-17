from fpdf import FPDF
class generatePDF:
  
  def __init__(self):
    
    self.pdf = FPDF(format='A4', unit='mm')
    self.pdf.add_page()
    self.pdf.set_font('Times','',10.0) 
    self.epw = self.pdf.w - 2*self.pdf.l_margin
    self.pdf.set_auto_page_break(True, 10)

    
  def write_text(self, text, texttype):
  
    if texttype == 'report header':
      self.pdf.ln(5)
      col_width = self.epw/2
      self.pdf.set_font('Times','B',15.0) 
      self.pdf.cell(self.epw, 0.0, text, align='C')
      self.pdf.ln(7)
      
    elif texttype == 'subheading':
      self.pdf.set_font('Times','B',12.0) 
      self.pdf.cell(self.epw, 0.0, text, align = 'C')
      self.pdf.set_font('Times','',12.0)
      self.pdf.ln(5)
      
    elif texttype == 'section heading':
      self.pdf.ln(6)
      self.pdf.set_font('Times','B',12.0) 
      self.pdf.cell(self.epw, 0.0, text)
      self.pdf.set_font('Times','',12.0) 
      
    elif texttype == 'normal text':
      self.pdf.ln(10)
      self.pdf.set_font('Times','',10.0)
      if text == 'Failed':
        self.pdf.set_text_color(255,0,0)
      self.pdf.cell(self.epw, 0.0, text)
      
      
      
  def display_sql_query(self, query):
    self.pdf.ln(5)
    self.pdf.set_font('Times','',10.0) 
    th = self.pdf.font_size
    self.pdf.multi_cell(185, 1.2*th, str(query), border=0, align= 'L')
    
  def create_table_summary(self,dictlist, column_size = ''):
    self.pdf.ln(5)
    self.pdf.set_font('Times','',10.0) 
    th = self.pdf.font_size
    if(column_size == 'L'):
      col_width_key = self.epw/2.5
    else:
      col_width_key = self.epw/3.3
    col_width_val = self.epw - col_width_key
      
    for key, value in dictlist.items(): 
      self.pdf.cell(col_width_key, 1.2*th, str(key), border=0, align= 'L')
      if (value == 'Failed' or (key == 'Reason' and value.find('mismatch') != -1)):
        self.pdf.set_text_color(255,0,0)
        
      data = ':' + str(value)
      self.pdf.multi_cell(col_width_val, 1.2*th, str(data), border = 0, align= 'L')
      self.pdf.set_text_color(0,0,0) 
  
  def create_table_details(self,df, table_type=''):
      self.pdf.ln(5)
      self.pdf.set_font('Times','B',12.0) 
      th = self.pdf.font_size
      if(df is None):
        self.pdf.set_font('Times','',12.0)
        self.pdf.cell(self.epw/7, th, str("None"), border=0, align = 'C')
        self.pdf.ln(2)
        self.pdf.set_font('Times','B',12.0) 
        return
      if(table_type == 'protocol'):
        col_width_list =[10,38,21,21,19,25,18,27,18]
        mth = 1.2*th   
      elif(table_type == 'protocol_count'):
        col_width_list =[10,60,21,21,19,25,18,27,18]
        mth = 1.2*th 
      elif(table_type == 'mismatch'):
        col_width_list = [10,100,0,0,30]
        mth = 1.2*th
      elif(table_type == 'mismatch_details'):
        #col_width_list = [10,30,30,30,40,40,10]
        col_width_list = [10,80,45,45]
        mth = 1.2*th
      elif(table_type == 'mismatch_summary'):
        col_width_list =[10,30,35,20,20,25,20,25,25]
        mth = 1.2*th
      else:
        col_width_list =[10,20,25,50,20,50,20]
        mth = 1.25*th
      col_width_reduced = [x - (0.1 * x) for x in col_width_list]
      table_header = df.columns[::-1]

      table_header.append('S.No')
      table_header = table_header[::-1]
      str_width =[self.pdf.get_string_width(str(i)) for i in table_header]
      factor_list = [i/j for (i,j) in zip(str_width,col_width_reduced)]
      factor_list.sort()
      factor = factor_list[-1]
      factor = int(factor)+1 if(factor>=int(factor)) else int(factor)
      cth = mth * factor   
      table_data = df.toPandas().values.tolist()

      for i,hd in enumerate(table_header):
        col_width = col_width_list[i]
        col_factor = self.pdf.get_string_width(str(hd))/col_width_reduced[i]   
        col_factor = int(col_factor)+1 if(col_factor>=int(col_factor)) else int(col_factor)
        x_pos = self.pdf.get_x()
        y_pos = self.pdf.get_y() 
        new_factor = factor / (col_factor + 1)
        cth = new_factor *mth #new
        
        if(col_factor == 1): #new
          self.pdf.cell(col_width, mth, str(hd), border=0, align = 'C')
          self.pdf.rect(self.pdf.get_x() - col_width,self.pdf.get_y() ,col_width,factor*mth)
        else: 
          self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
          self.pdf.multi_cell(col_width, cth, str(hd), border = 0, align= 'C') #new
          self.pdf.set_xy(x_pos + col_width, y_pos)
      cth = factor * mth
      self.pdf.ln(cth)

      self.pdf.set_font('Times','',10.0) 
      th = self.pdf.font_size
      
      for i,row in enumerate(table_data):
        str_width =[self.pdf.get_string_width(str(i)) for i in row]
        factor_list = [i/j for (i,j) in zip(str_width,col_width_reduced[1:])]
        factor_list.sort()
        factor = factor_list[-1]
        factor = int(factor)+1 if(factor>=int(factor)) else int(factor)
        cth = factor * mth  
        s_no= i+1
        k=0
        self.pdf.cell(col_width_list[0], cth, str(s_no), border=0, align = 'C')
        x_pos = self.pdf.get_x() - col_width_list[0]
        y_pos = self.pdf.get_y()
        self.pdf.rect(x_pos, y_pos,col_width_list[0],factor*mth)
        for j,val in enumerate(row):
            k = j+1
            col_width = col_width_list[k]
            
            if(val == 'Failed'):
              self.pdf.set_text_color(255,0,0)
            col_factor = self.pdf.get_string_width(str(val))/col_width_reduced[k]   
            col_factor = int(col_factor)+1 if(col_factor>=int(col_factor)) else int(col_factor) 
            x_pos = self.pdf.get_x()
            y_pos = self.pdf.get_y() 
            new_factor = factor / (col_factor + 1)
            cth = new_factor * mth 
            if(col_factor == 1): 
              if(table_header[k] == 'Key Columns' or table_header[k] == 'Testcase Name'):
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.cell(col_width, mth, str(val), border=0, align = 'L')
              else:
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.cell(col_width, mth, str(val), border=0, align = 'C') 
            else: 
              if(table_header[k] == 'Key Columns' or table_header[k] == 'Testcase Name'):
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.multi_cell(col_width, cth, str(val), border = 0, align= 'L') 
              else:
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.multi_cell(col_width, cth, str(val), border = 0, align= 'C')
              self.pdf.set_xy(x_pos + col_width, y_pos)
            
            self.pdf.set_text_color(0,0,0)
              
        cth = factor * mth
        self.pdf.ln(cth)
      self.pdf.ln(5)
      
  #Function for Schema Comparator    
  def create_table_schemacomp(self, df, col_width_list, table_type=''):
      self.pdf.ln(5)
      self.pdf.set_font('Times','B',10.0) 
      th = self.pdf.font_size
      if(df is None):
        self.pdf.set_font('Times','',10.0)
        self.pdf.cell(self.epw/7, th, str("None"), border=0, align = 'L')
        self.pdf.ln(2)
        self.pdf.set_font('Times','B',10.0) 
        return
      if(table_type == 'protocol'):
        col_width_list = col_width_list
        mth = 1.5*th   
      elif(table_type == 'mismatch'):
        col_width_list = col_width_list
        mth = 2*th
      elif(table_type == 'match'):
        col_width_list = col_width_list
        mth = 2*th
      elif(table_type == 'source_only'):
        col_width_list = col_width_list
        mth = 2*th
      elif(table_type == 'target_only'):
        col_width_list = col_width_list
        mth = 2*th
      elif(table_type == 'mismatch_details'):
        col_width_list = col_width_list
        mth = 1.2*th
      elif(table_type == 'mismatch_summary'):
        col_width_list =[10,30,35,20,20,25,20,25,25]
        mth = 1.2*th
      else:
        col_width_list =[10,20,25,50,20,50,20]
        mth = 1.25*th
      col_width_reduced = [x - (0.1 * x) for x in col_width_list]
      table_header = df.columns[::-1]

      table_header.append('S.No')
      table_header = table_header[::-1]
      str_width =[self.pdf.get_string_width(str(i)) for i in table_header]
      factor_list = [i/j for (i,j) in zip(str_width,col_width_reduced)]
      factor_list.sort()
      factor = factor_list[-1]
      factor = int(factor)+1 if(factor>=int(factor)) else int(factor)
      cth = mth * factor   # Cell height
      table_data = df.toPandas().values.tolist()
      
      #Looping for the header fields
      for i,hd in enumerate(table_header):
        col_width = col_width_list[i]
        col_factor = self.pdf.get_string_width(str(hd))/col_width_reduced[i]   
        col_factor = int(col_factor)+1 if(col_factor>=int(col_factor)) else int(col_factor)
        x_pos = self.pdf.get_x()
        y_pos = self.pdf.get_y() 
        new_factor = factor / (col_factor + 2)
        cth = new_factor *mth #new
        
        if(col_factor == 1): #new
          self.pdf.rect(self.pdf.get_x(),self.pdf.get_y() ,col_width,factor*mth)
          self.pdf.cell(col_width, mth, str(hd), border=0, align = 'C')
        else: 
          self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
          self.pdf.multi_cell(col_width, cth, str(hd), border = 0, align= 'C') #new
          self.pdf.set_xy(x_pos + col_width, y_pos)
            
      cth = factor * mth
      self.pdf.ln(cth)

      self.pdf.set_font('Times','',10.0) #Sid : Changed font from 11 to 10
      th = self.pdf.font_size
      
      #Printing the data
      for i,row in enumerate(table_data):
#         if i == 0:
        str_width =[self.pdf.get_string_width(str(i)) for i in row]
        factor_list = [i/j for (i,j) in zip(str_width,col_width_reduced[1:])]
        factor_list.sort()
        factor = factor_list[-1]
        factor = int(factor)+1 if(factor>=int(factor)) else int(factor)
        cth = factor * mth  
        s_no= i+1
        k=0
        self.pdf.cell(col_width_list[0], cth, str(s_no), border=0, align = 'C')
        x_pos = self.pdf.get_x() - col_width_list[0]
        y_pos = self.pdf.get_y()
        self.pdf.rect(x_pos, y_pos,col_width_list[0],factor*mth)
        for j,val in enumerate(row):
            k = j+1
            col_width = col_width_list[k]
            
            if(val == 'Failed'):
              self.pdf.set_text_color(255,0,0)
            col_factor = self.pdf.get_string_width(str(val))/col_width_reduced[k] 
            col_factor = int(col_factor)+1 if(col_factor>=int(col_factor)) else int(col_factor) 
            x_pos = self.pdf.get_x()
            y_pos = self.pdf.get_y() 
            new_factor = factor / (col_factor) #+1)
            cth = new_factor * mth 
            if(col_factor == 1):
              if(table_header[k] == 'Key Columns' or table_header[k] == 'Testcase Name'):
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.cell(col_width, cth, str(val), border=0, align = 'L')
              else:
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.cell(col_width, cth, str(val), border=0, align = 'C') #new
            else: 
              if(table_header[k] == 'Key Columns' or table_header[k] == 'Testcase Name'):
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.multi_cell(col_width, cth, str(val), border = 0, align= 'L') #new
              else:
                self.pdf.rect(x_pos,y_pos,col_width,factor*mth)
                self.pdf.multi_cell(col_width, cth, str(val), border = 0, align= 'C')
              self.pdf.set_xy(x_pos + col_width, y_pos)
            
            #resetting font color back to automatic after printing Failed in Red
            self.pdf.set_text_color(0,0,0)
              
        cth = factor * mth
        self.pdf.ln(cth)
      self.pdf.ln(5)