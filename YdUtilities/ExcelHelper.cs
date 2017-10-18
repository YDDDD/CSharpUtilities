using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;

using NPOI.SS.UserModel;
using NPOI.HSSF.UserModel;
using NPOI.XSSF.UserModel;

using YdUtilities.Logger;
using YdUtilities.Constants;

namespace YdUtilities
{
    public static class ExcelHelper
    {
        public static DataTable DataFromExcelFileWithTitle(string filePath)
        {
            string errMsg;
            return DataFromExcelFileWithTitle(filePath, out errMsg);
        }

        public static DataTable DataFromExcelFileWithTitle(string filePath, out string errMsg)
        {
            DataTable dt = null;
            errMsg = string.Empty;
            int i = -1, j = -1;//为记录日志，在TRYCATCH之外声明

            try
            {
                FileStream fs = File.Open(filePath, FileMode.Open);
                IWorkbook workbook = null;
                string fileExt = Path.GetExtension(filePath);
                if (fileExt == ".xls")
                    workbook = new HSSFWorkbook(fs);
                else if (fileExt == ".xlsx")
                    workbook = new XSSFWorkbook(fs);
                else
                    throw new Exception("Invalid file type");

                ISheet sheet = workbook.GetSheetAt(0);

                int rowCount = sheet.PhysicalNumberOfRows;
                int colCount = sheet.GetRow(0).PhysicalNumberOfCells;

                dt = new DataTable();

                for (i = 0; i < colCount; i++)
                {
                    dt.Columns.Add(sheet.GetRow(0).GetCell(i).ToString());
                }

                for (i = 1; i < rowCount; i++)
                {
                    DataRow dr = dt.NewRow();
                    for (j = 0; j < colCount; j++)
                    {
                        dr[j] = sheet.GetRow(i).GetCell(j) == null ? string.Empty : sheet.GetRow(i).GetCell(j).ToString();
                    }
                    dt.Rows.Add(dr);
                }
            }
            catch (Exception e)
            {
                if (i == -1 && j == -1)
                    errMsg = string.Format("ERROR at Import Excel File : {0}; {1}", filePath, e.Message);
                else
                    errMsg = string.Format("ERROR at Import Excel File : {0} at Row:Col {1}:{2};", filePath, i, j);
                LogHelper.logError(errMsg, e);
                dt = null;
            }

            return dt;
        }

        public static DataTable RawDataFromExcelFile(string fileName, string folderPath)
        {
            string errMsg;
            return RawDataFromExcelFile(fileName, folderPath, out errMsg);
        }

        public static DataTable RawDataFromExcelFile(string fileName, string folderPath, out string errMsg)
        {
            return RawDataFromExcelFile(Path.Combine(folderPath, fileName), out errMsg);
        }

        public static DataTable RawDataFromExcelFile(string filePath)
        {
            string errMsg;
            return RawDataFromExcelFile(filePath, out errMsg);
        }

        public static DataTable RawDataFromExcelFile(string filePath, out string errMsg)
        {
            DataTable dt = null;
            errMsg = string.Empty;
            int i = -1, j = -1;//为记录日志，在TRYCATCH之外声明

            try
            {
                FileStream fs = File.Open(filePath, FileMode.Open);
                IWorkbook workbook = null;
                string fileExt = Path.GetExtension(filePath);
                if (fileExt == ".xls")
                    workbook = new HSSFWorkbook(fs);
                else if (fileExt == ".xlsx")
                    workbook = new XSSFWorkbook(fs);
                else
                    throw new Exception("Invalid file type");

                ISheet sheet = workbook.GetSheetAt(0);

                int rowCount = sheet.PhysicalNumberOfRows;
                int colCount = sheet.GetRow(0).PhysicalNumberOfCells;

                dt = new DataTable();

                for (i = 0; i < colCount; i++)
                {
                    dt.Columns.Add(i.ToString());
                }

                for (i = 0; i < rowCount; i++)
                {
                    DataRow dr = dt.NewRow();
                    for (j = 0; j < colCount; j++)
                    {
                        dr[j] = sheet.GetRow(i).GetCell(j) == null ? string.Empty : sheet.GetRow(i).GetCell(j).ToString();
                    }
                    dt.Rows.Add(dr);
                }
            }
            catch (Exception e)
            {
                if (i == -1 && j == -1)
                    errMsg = string.Format("ERROR at Import Excel File : {0}; {1}", filePath, e.Message);
                else
                    errMsg = string.Format("ERROR at Import Excel File : {0} at Row:Col {1}:{2};", filePath, i, j);
                LogHelper.logError(errMsg, e);
                dt = null;
            }

            return dt;
        }

        public static int ExportXlsFile(DataTable dt, string filePath)
        {
            string errMsg;
            return ExportXlsFile(dt, filePath, out errMsg);
        }

        public static int ExportXlsFile(DataTable dt, string filePath, out string errMsg)
        {
            int ret = ConstantManagement.ERROR;
            errMsg = string.Empty;
            FileStream fs = null;

            HSSFWorkbook workbook = new HSSFWorkbook();
            HSSFSheet sheet = (HSSFSheet)workbook.CreateSheet();
            HSSFRow hssfRow = (HSSFRow)sheet.CreateRow(0);

            foreach (DataColumn col in dt.Columns)
            {
                hssfRow.CreateCell(col.Ordinal).SetCellValue(col.ColumnName);
            }

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                hssfRow = (HSSFRow)sheet.CreateRow(i + 1);
                for (int j = 0; j < dt.Columns.Count; j++)
                {
                    hssfRow.CreateCell(j).SetCellValue(dt.Rows[i][j].ToString());
                }
            }

            try
            {
                fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
                workbook.Write(fs);
                ret = ConstantManagement.OK;
            }
            catch (Exception e)
            {
                errMsg = string.Format("XLS文件导出出错：{0} ", filePath);
                LogHelper.logError(errMsg, e);
            }
            finally
            {
                if (fs != null) fs.Close();

            }

            return ret;
        }

    }
}
