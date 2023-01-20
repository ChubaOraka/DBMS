using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Net.Mail;
using System.Reflection;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SharedUtilities;
using SKSAICore.LibraryCode.General;

namespace LightningBolt.UtilityBelt.CMC

{
    public class DBMSOperation
    {
        public class TransactionContext
        {
            public SqlCommand Cmd { get; set; }
            public MethodBase MthdBs { get; set; }
            public bool Success { get; set; }
            public Stopwatch sw { get; set; }
            public StringBuilder sbSqlDeets { get; set; }

        }
        public static string TableLogQuery = "[LogQuery]";
        public static string ContextDefault = "ICAN2"; // TODO: add delegate function that return string
        public static string EnvironmentVar = "";
        public static IConfiguration Configuration = null;
        public static HttpContext HttpContext = null;


        private static void ParameterSetup(ref SqlCommand parmSqlCmd, params object[] parmSP)
        {
            bool parametersExist = parmSP != null;
            if (parmSP!.Length == 0) parametersExist = false;
            if (!parametersExist) return;
            if (parmSP.Length % 2 != 0)
            {
                // TODO: should include bad parameter pairs diagnostic info
                throw new Exception("parameters are not paired");
            }

            for (int countParams = 0; countParams < parmSP.Length; countParams += 2)
            {
                SqlParameter param = parmSqlCmd.CreateParameter();
                param.ParameterName = parmSP[countParams].ToString();
                param.Direction = ParameterDirection.Input;
                param.Value = parmSP[countParams + 1];
                parmSqlCmd.Parameters.Add(param);
            }
        }

        private static void ParameterSetup(ref TransactionContext parmTxContext, params object[] parmSP)
        {
            bool parametersExist = parmSP != null;
            if (parmSP!.Length == 0) parametersExist = false;
            if (!parametersExist) return;
            if (parmSP.Length % 2 != 0)
            {
                // TODO: should include bad parameter pairs diagnostic info
                throw new Exception("parameters are not paired");
            }
            if (parmTxContext.Cmd.Parameters.Count > 0)
            {
                parmTxContext.Cmd.Parameters.Clear();
            }
            for (int countParams = 0; countParams < parmSP.Length; countParams += 2)
            {
                SqlParameter param = parmTxContext.Cmd.CreateParameter();
                param.ParameterName = parmSP[countParams].ToString();
                param.Direction = ParameterDirection.Input;
                param.Value = parmSP[countParams + 1];
                parmTxContext.Cmd.Parameters.Add(param);
            }
        }

        public static void BulkInsert(MethodBase parmMB, string parmConnString, DataTable parmDT, string parmSqlTableName, int Timeout = 10000, int BatchSize = 100000, SqlBulkCopyOptions BulkCopyOptions = SqlBulkCopyOptions.Default)
        {
            var sw = new Stopwatch();
            sw.Start();
            using SqlConnection conn = ConnectionOpen(parmMB, parmConnString);
            using var bulkCopy = new SqlBulkCopy(conn, BulkCopyOptions, null)
            {
                BulkCopyTimeout = Timeout,
                BatchSize = BatchSize,
                DestinationTableName = parmSqlTableName
            };
            try
            {
                //  this just gives the BulkCopy mappings straight from datatable schema
                // exception: The given ColumnMapping does not match up with any column in the source or destination.'
                // almost always means fieldName in dataTable column did not match case in database
                foreach (DataColumn dc in parmDT.Columns)
                {
                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(dc.ColumnName, dc.ColumnName));
                }
                // var whatever = bulkCopy.WriteToServerAsync(parmDT.CreateDataReader());
                bulkCopy.WriteToServer(parmDT.CreateDataReader());
                sw.Stop();
                Debug.WriteLine($"bulkCopy success :) {parmDT.Rows.Count} records ingested is {sw.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                sw.Stop();
                Debug.WriteLine(Lumberjack.PrettyExceptionDetails(ex));
                Debug.WriteLine($"bulkCopy FAIL :( {parmDT.Rows.Count} records NOT ingested in {sw.ElapsedMilliseconds}ms)");
                throw;
            }
        }


        public static void BulkInsert(TransactionContext parmTxCntxt, DataTable parmDT, string parmSqlTableName, int Timeout = 10000, int BatchSize = 100000, SqlBulkCopyOptions BulkCopyOptions = SqlBulkCopyOptions.Default)
        {
            if (parmTxCntxt.Success == false) { return; }
            var sw = new Stopwatch();
            sw.Start();
            using var bulkCopy = new SqlBulkCopy(parmTxCntxt.Cmd.Connection, BulkCopyOptions, parmTxCntxt.Cmd.Transaction)
            {
                BulkCopyTimeout = Timeout,
                BatchSize = BatchSize,
                DestinationTableName = parmSqlTableName
            };
            try
            {
                //  this just gives the BulkCopy mappings straight from datatable schema
                // exception: The given ColumnMapping does not match up with any column in the source or destination.'
                // almost always means fieldName in dataTable column did not match case in database
                foreach (DataColumn dc in parmDT.Columns)
                {
                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(dc.ColumnName, dc.ColumnName));
                }
                // var whatever = bulkCopy.WriteToServerAsync(parmDT.CreateDataReader());
                bulkCopy.WriteToServer(parmDT.CreateDataReader());
                sw.Stop();
                Debug.WriteLine($"bulkCopy success :) {parmDT.Rows.Count} records ingested is {sw.ElapsedMilliseconds}ms");
            }
            catch (Exception exc1)
            {
                parmTxCntxt.Success = false;
                Debug.WriteLine(Lumberjack.PrettyExceptionDetails(exc1));
                Debug.WriteLine($"bulkCopy FAIL :( {parmDT.Rows.Count} records NOT ingested in {sw.ElapsedMilliseconds}ms)");
                //never throw; it makes it not possible for TxEnd to be called!
            }
        }
        public static SqlConnection ConnectionOpen(MethodBase parmMB, string parmDBMSConnect)
        {
            SqlConnection conn = null;
            conn = new SqlConnection();
            conn.ConnectionString = ConnectionStringRead(parmDBMSConnect);
            conn.Open();
            if (conn == null)
            {
                Debug.WriteLine($"Class and Method: {parmMB.DeclaringType.FullName} {parmMB.Name}");
                Debug.WriteLine($"Connection NOT opening {conn.ConnectionString}");
                return conn;
            }
            return conn;
        }


        public static string ConnectionStringRead(string parmConnect)
        {
            return Configuration.GetConnectionString(parmConnect);
        }

        public static string ConnectionDbName(string parmConnect)
        {
            var tempReturn = ConnectionStringRead(parmConnect);
            int posDataSource = tempReturn.IndexOf("Data Source=");
            int posSemicolonAfter = tempReturn.IndexOf(";", posDataSource + 1) - posDataSource - 12;
            tempReturn = tempReturn.Substring(posDataSource + 12, posSemicolonAfter);
            return tempReturn;
        }


        private static SqlCommand ConnectCommandSetup(MethodBase parmMB, string parmDBMSConnect, string parmQuery, params object[] parmSP)
        {
            SqlConnection conn = ConnectionOpen(parmMB, parmDBMSConnect);
            SqlCommand cmd = new SqlCommand(parmQuery, conn);
            ParameterSetup(ref cmd, parmSP);
            return cmd;
        }

        public static int Exec(MethodBase parmMB, string parmDBMSConnect, string parmQuery, params object[] parmSP)
        {
            int tempReturn = -1;
            SqlConnection conn = null;
            SqlCommand cmd = null;
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                cmd = ConnectCommandSetup(parmMB, parmDBMSConnect, parmQuery, parmSP);
                tempReturn = cmd.ExecuteNonQuery();
            }
            catch (Exception exc1)
            {
                LogException(parmMB, exc1, cmd);
            }
            finally
            {
                sw.Stop();
                conn?.Dispose();
                cmd?.Dispose();
                string msg = $"Exec recsAffected={tempReturn}";
                LogQuery(parmMB, msg, cmd, sw, tempReturn, -1);
            }
            return tempReturn;
        }

        public static int Exec(TransactionContext parmTxCntxt, string parmQuery, params object[] parmSP)
        {
            int tempReturn = -1;
            if (parmTxCntxt.Success == false)
            { return -1; }
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                parmTxCntxt.Cmd.CommandText = parmQuery;
                ParameterSetup(ref parmTxCntxt, parmSP);
                tempReturn = parmTxCntxt.Cmd.ExecuteNonQuery();
            }
            catch (Exception exc1)
            {
                LogException(parmTxCntxt.MthdBs, exc1, parmTxCntxt.Cmd);
                parmTxCntxt.Success = false;
            }
            finally
            {
                sw.Stop();
                string msg = $"Exec recsAffected={tempReturn}";
                LogQuery(parmTxCntxt.MthdBs, msg, parmTxCntxt.Cmd, sw, tempReturn, -1, parmTxCntxt: parmTxCntxt);
            }
            return tempReturn;
        }

        public static SqlDataReader GetDataReader(MethodBase parmMB, string parmDBMSConnect, string parmQuery, params object[] parmSP)
        {
            SqlDataReader tempDR = null;
            SqlCommand cmd = null;
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                cmd = ConnectCommandSetup(parmMB, parmDBMSConnect, parmQuery, parmSP);
                tempDR = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                var tempDR2 = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                var tempDT = new DataTable();
                if (!tempDR.IsClosed)
                {
                    if (tempDR.HasRows)
                    {
                        tempDT.Load(tempDR2);
                        Debug.WriteLine(PrettyDataTable(tempDT, null));
                    }
                }
            }
            catch (Exception exc1)
            {
                LogException(parmMB, exc1, cmd);
                throw;
            }
            finally
            {
                cmd?.Dispose();
                sw.Stop();
            }
            string msg = tempDR.HasRows ? $"get DataReader with {tempDR.VisibleFieldCount} cols" : msg = $"empty DataReader, no rows :(";
            LogQuery(parmMB, msg, cmd, sw, -1, tempDR.VisibleFieldCount);
            return tempDR;
        }

        public static DataTable GetDataTable(MethodBase parmMB, string parmDBMSConnect, string parmQuery, params object[] parmSP)
        {
            DataTable tempDT = new DataTable();
            SqlConnection conn = null;
            SqlCommand cmd = null;
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                cmd = ConnectCommandSetup(parmMB, parmDBMSConnect, parmQuery, parmSP);
                tempDT.Load(cmd.ExecuteReader());
                Debug.WriteLine(PrettyDataTable(tempDT, null));
            }
            catch (Exception exc1)
            {
                LogException(parmMB, exc1, cmd);
                throw;
            }
            finally
            {
                sw.Stop();
                conn?.Dispose();
                cmd?.Dispose();
            }
            string msg = tempDT.Rows.Count > 0 ? $"get DataTable {tempDT.Rows.Count} rows x {tempDT.Columns.Count} cols, {tempDT.Rows.Count * tempDT.Columns.Count} cells" :
                $"empty DataTable no rows :(";
            LogQuery(parmMB, msg, cmd, sw, tempDT.Rows.Count, tempDT.Columns.Count);
            return tempDT;
        }

        public static TypeOfScalar GetScalar<TypeOfScalar>(MethodBase parmMB, string parmDBMSConnect, string parmQuery, params object[] parmSP)
        {
            object tempReturn = null;
            SqlConnection conn = null;
            SqlCommand cmd = null;
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                cmd = ConnectCommandSetup(parmMB, parmDBMSConnect, parmQuery, parmSP);
                tempReturn = (cmd.ExecuteScalar());
                string msg = $"GetScalar: result={tempReturn}";
                LogQuery(parmMB, msg, cmd, sw);
                if (typeof(TypeOfScalar) == typeof(Int32))
                {
                    tempReturn = Convert.ToInt32(tempReturn);
                }
                return (TypeOfScalar)tempReturn;
            }
            catch (Exception exc1)
            {
                LogException(parmMB, exc1, cmd);
                return (TypeOfScalar)tempReturn;
                throw;
            }
            finally
            {
                sw.Stop();
                conn?.Dispose();
                cmd?.Dispose();
            }
        }

        public static TypeOfScalar GetScalar<TypeOfScalar>(TransactionContext parmTxCntxt, string parmQuery, params object[] parmSP)
        {
            if (parmTxCntxt.Success == false)
            {
                return default(TypeOfScalar);
            }
            object tempReturn = null;
            string msg = String.Empty;
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                parmTxCntxt.Cmd.CommandText = parmQuery;
                ParameterSetup(ref parmTxCntxt, parmSP);
                tempReturn = (parmTxCntxt.Cmd.ExecuteScalar());
                msg = $"GetScalar: result={tempReturn}";
                return (TypeOfScalar)tempReturn;
            }
            catch (Exception exc1)
            {
                LogException(parmTxCntxt.MthdBs, exc1, parmTxCntxt.Cmd);
                parmTxCntxt.Success = false;
            }
            finally
            {
                sw.Stop();
                LogQuery(parmTxCntxt.MthdBs, msg, parmTxCntxt.Cmd, sw, parmTxCntxt: parmTxCntxt);
            }
            return default(TypeOfScalar);
        }


        private static void LogException(MethodBase parmMB, Exception parmExc, SqlCommand parmCmd)
        {
            StringBuilder msgException = new StringBuilder();
            msgException.AppendLine($"Class and Method: {parmMB.DeclaringType.FullName} {parmMB.Name}");
            msgException.AppendLine(parmExc.Message);
            msgException.AppendLine(Lumberjack.PrettyExceptionDetails(parmExc));
            if (parmCmd != null)
            {
                msgException.AppendLine(SqlFromCmd(parmCmd));
            }
            Debug.Write(msgException.ToString());
            LogSendEmail(parmMB, parmExc, parmCmd);
        }

        private static (string host, string url, string userLoginName, string userDisplayName) SiteInfo()
        {
            var host = HttpContext.Request.Host.Value;
            var url = HttpContext.Request.Scheme + "://" + host + HttpContext.Request.Path.Value;
            var userDisplayName = AdUtils.GetUserDisplayName(HttpContext);
            var userLoginName = AdUtils.GetUserLoginName(HttpContext);
            return (host, url, userLoginName, userDisplayName);
        }

        private static void LogSendEmail(MethodBase parmMB, Exception parmExc, SqlCommand parmCmd)
        {
            var siteDeets = SiteInfo();
            StringBuilder msgDetails = new StringBuilder();
            msgDetails.AppendLine($"User: {siteDeets.userLoginName} / {siteDeets.userDisplayName}");
            msgDetails.AppendLine();
            msgDetails.AppendLine($"ErrorURL: {siteDeets.url}");
            msgDetails.AppendLine();
            msgDetails.AppendLine($"Class and Method: {parmMB.DeclaringType.FullName} {parmMB.Name}");
            msgDetails.AppendLine();
            if (parmCmd != null)
            {
                msgDetails.AppendLine(SqlFromCmd(parmCmd));
            }
            msgDetails.AppendLine();
            msgDetails.AppendLine(Lumberjack.PrettyExceptionDetails(parmExc));

            var recipient = Configuration["ErrEmail"];
            string subject = $"Exception for ICANv2 {siteDeets.host} SQL Error";
            SendSmtp("noreply@cookchildrens.local", recipient, subject, msgDetails.ToString(), false);
        }

        private static void LogQuery(MethodBase parmMB, string parmMsg, SqlCommand parmCmd, Stopwatch parmSW,
            int parmRowCount = -1, int parmColCount = -1, TransactionContext parmTxCntxt = null)

        {
            var MB = MethodBase.GetCurrentMethod();
            string context = ContextDefault;
            int slowQueryThreshold = Convert.ToInt32(Configuration["SlowQueryThreshold"]);
            //Debug.WriteLine($"QueryTook {parmSW.ElapsedMilliseconds}ms; slowQueryThreshold={slowQueryThreshold}ms");
            if (parmCmd.CommandText.ToLower().IndexOf(TableLogQuery.ToLower()) > 0)
            {
                // vital to avoid infinite recursion :| As the logQuery logs the writing of the log
                return;
            }
            var querySlowSuffix = parmSW.ElapsedMilliseconds > slowQueryThreshold ? $"-- Slow Query >{slowQueryThreshold}ms;" : "";
            var sbBetterText = new StringBuilder();
            sbBetterText.Append(QueryDetails(parmMB, parmMsg, parmCmd, parmSW, parmRowCount, parmColCount) + querySlowSuffix);
            Debug.WriteLine(sbBetterText.ToString());
            if (parmTxCntxt != null)
            {
                parmTxCntxt.sbSqlDeets.Append(sbBetterText.ToString());
            }
            if (parmSW.ElapsedMilliseconds < slowQueryThreshold)
            {
                return;
            }
            string logQuery = $"Insert Into [LogQuery] ([Started],[Elapsed],[QueryExecuted],[Context],[RowCount],[ColCount])";
            logQuery += "Values(";
            logQuery += $"'{DateTime.Now}'";
            logQuery += $",{parmSW.ElapsedMilliseconds}";
            logQuery += $",'{sbBetterText.ToString().Replace("'", "''")}'";
            logQuery += $",'{context}',{parmRowCount},{parmColCount})";

            Exec(MB, "SKSAI", logQuery);
        }

        private static string QueryDetails(MethodBase parmMB, string parmMsg, SqlCommand parmCmd, Stopwatch parmSW,
            int parmRowCount = -1, int parmColCount = -1)
        {
            StringBuilder sbDetails = new StringBuilder();
            StackTrace callStack = new StackTrace(1, true);
            StackFrame callingMethodFrame = callStack.GetFrame(3);
            var caller = callingMethodFrame.GetMethod();
            sbDetails.AppendLine($"-- {caller.DeclaringType?.FullName} / {caller.Name}");
            sbDetails.AppendLine($"-- {parmMB.DeclaringType?.FullName}/{parmMB.Name}");
            sbDetails.AppendLine("-- Query Start");
            sbDetails.AppendLine(SqlFromCmd(parmCmd));
            sbDetails.AppendLine($"-- Query End {parmMsg} in {parmSW.ElapsedMilliseconds}ms");
            var tempDetails = sbDetails.ToString();
            return tempDetails;
        }

        public static string PrettyDataTable(DataTable pDtbl, DataTable Schema = null)
        {
            string textLongCell = "<long text, length=xxxxxxx chars>";
            string textNullDataTable = "<DataTable is null>";
            string textEmptyDataTable = "<DataTable has 0 rows";

            int columnNumber = 0;
            int maxColumnSizeToDisplay = 75;
            if (pDtbl == null) return textNullDataTable;
            if (pDtbl.Rows.Count == 0) return textEmptyDataTable;

            // calculate column length based on data
            var colLengthMax = new Dictionary<int, int>();
            foreach (DataRow dr in pDtbl.Rows)
            {
                columnNumber = 0;
                foreach (DataColumn c in pDtbl.Columns)
                {
                    object objCell = dr[c.ColumnName];
                    string strCell = dr[c.ColumnName].ToString().Trim();
                    var lengthOfCell = strCell.Length;
                    if (c.ColumnName == "FieldText")
                    {
                        // breakpoint
                    }
                    if (objCell is DBNull && lengthOfCell < 8)
                    {
                        lengthOfCell = 8;
                    }
                    if (!colLengthMax.ContainsKey(columnNumber))
                    {
                        colLengthMax.Add(columnNumber, lengthOfCell);
                    }
                    if (lengthOfCell > colLengthMax[columnNumber])
                    {
                        colLengthMax[columnNumber] = lengthOfCell;

                        if (lengthOfCell > maxColumnSizeToDisplay)
                        {
                            colLengthMax[columnNumber] = textLongCell.Length + 4;
                        }

                    }
                    columnNumber++;
                }
            }

            // This code looks at column name and re-sizes if name is larger than data length
            columnNumber = 0;
            foreach (DataColumn c in pDtbl.Columns)
            {
                if (c.ColumnName.Length > colLengthMax[columnNumber])
                {
                    colLengthMax[columnNumber] = c.ColumnName.Length + 4;
                }

                columnNumber++;
            }

            // This code looks at Data and re-sizes column sizes based on data

            // now put column headings into StringBuilder
            var sbColumnHeadings = new StringBuilder();
            columnNumber = 0;
            foreach (DataColumn c in pDtbl.Columns)
            {
                sbColumnHeadings.AppendFormat("{0} | ", c.ColumnName.PadRight(colLengthMax[columnNumber]));
                columnNumber++;
            }
            sbColumnHeadings.AppendLine();

            // now we are ready to throw data into string
            var sbDataTable = new StringBuilder();
            long lengthOfData = 0;
            foreach (DataRow dr in pDtbl.Rows)
            {
                columnNumber = 0;
                foreach (DataColumn c in pDtbl.Columns)
                {
                    object tempCell = dr[c.ColumnName];
                    string strCell = tempCell.ToString();
                    int thisCellLength = strCell.Trim().Length;
                    if (thisCellLength > maxColumnSizeToDisplay)
                    {
                        strCell = "<long text, length=" + thisCellLength + " chars>";
                        thisCellLength = textLongCell.Length + 4;
                    }

                    if (tempCell is DBNull)
                    {
                        strCell = "null";
                        lengthOfData += 1;
                    }
                    lengthOfData += thisCellLength;

                    // pad Column to length
                    sbDataTable.AppendFormat("{0} | ", strCell.PadRight(colLengthMax[columnNumber]));
                    columnNumber++;
                }
                sbDataTable.AppendLine();

                //Debug.WriteLine(sbDataTable.ToString());
            }
            columnNumber = 0;

            // now we can build the Data Display since column lengths are set

            // now put it all together
            string extraInfo = string.Format("<data display> rows={0}; cols={1}; CellCount={2}; Length={3} ", pDtbl.Rows.Count,
                pDtbl.Columns.Count, pDtbl.Rows.Count * pDtbl.Columns.Count, lengthOfData);
            return extraInfo + Environment.NewLine + sbColumnHeadings.ToString() + sbDataTable.ToString();
        }


        public static string PrettySql(string pSQL)
        {
            // TODO RegEx
            StringBuilder sbQuery = new StringBuilder();
            sbQuery.Append(pSQL);
            string newLinePlusIndent = System.Environment.NewLine + "   ";
            //sbQuery.Replace(", ", "," + System.Environment.NewLine + "   "); // this gets split isnull(u.lastname,'')+', '+isnull(u.firstname,'') AS Coordinator_Name,
            sbQuery.Replace(",   ", "," + newLinePlusIndent);
            sbQuery.Replace("INSERT INTO", newLinePlusIndent + "INSERT INTO");
            sbQuery.Replace("FROM", newLinePlusIndent + "FROM");
            sbQuery.Replace("LEFT OUTER JOIN", newLinePlusIndent + "LEFT OUTER JOIN");
            sbQuery.Replace("INNER JOIN", newLinePlusIndent + "INNER JOIN");
            sbQuery.Replace("FULL JOIN", newLinePlusIndent + "FULL JOIN");
            sbQuery.Replace("SET", newLinePlusIndent + "SET");
            sbQuery.Replace("VALUES", newLinePlusIndent + "VALUES");
            sbQuery.Replace("WHERE", newLinePlusIndent + "WHERE");
            sbQuery.Replace("ORDER BY", newLinePlusIndent + "ORDER BY");
            return sbQuery.ToString();

        }

        public bool ReadConfigThrowError(MethodBase parmMB)
        {
            return false;
        }

        private static bool SendSmtp(string from, string recipients, string subject, string msg, bool isHtml)
        {
            try
            {
                // if there are no recipient(s) return false
                if (string.IsNullOrWhiteSpace(recipients))
                {
                    return false;
                }

                if (from == "")
                    from = "noreply@cookchildrens.org";

                // prep email

                msg = msg.Replace(System.Environment.NewLine, "\r\n");
                MailMessage mail = new MailMessage
                {
                    Subject = subject,
                    Body = msg,
                    IsBodyHtml = isHtml,
                    From = new MailAddress(from)
                };

                // split the email list, and add them one by one as recipient objects
                string[] recipientsArray = recipients.Split(',');
                foreach (string recipient in recipientsArray)
                {
                    mail.To.Add(recipient);
                }

                // instantiate mail server
                SmtpClient smtpServer = new SmtpClient
                {
                    Host = "cchcscooksmtp.cchcs.ldap"
                ,
                    Credentials = new NetworkCredential("CCHCS\\WebDevAppTest.svc", "Blik_02!")
                };

                Debug.WriteLine("");
                Debug.WriteLine("Que Exception Email!");
                Debug.WriteLine($"TO:\t{subject}");
                Debug.WriteLine($"FROM:\t{from}");
                Debug.WriteLine($"SUBJECT:\t{subject}");
                Debug.WriteLine($"BODY: {msg}");

                smtpServer.Send(mail);

                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(Lumberjack.PrettyExceptionDetails(ex));
                return false;
            }

        }

        private static string SqlFromCmd(SqlCommand cmd)
        {
            try
            {
                StringBuilder commandTxt = new StringBuilder();
                if (cmd.Parameters.Count > 0) commandTxt.Append("DECLARE ");
                List<string> paramlst = new List<string>();
                int parameterCount = 0;
                foreach (SqlParameter parms in cmd.Parameters)
                {
                    paramlst.Add(parms.ParameterName);
                    commandTxt.Append(parms.ParameterName + " AS ");
                    commandTxt.Append(parms.SqlDbType.ToString());
                    commandTxt.Append(",");
                    parameterCount++;
                }
                if (parameterCount > 0)
                {
                    if (commandTxt.ToString().Substring(commandTxt.Length - 1, 1) == ",")
                        commandTxt.Remove(commandTxt.Length - 1, 1);
                }
                //commandTxt.AppendLine();
                int rownr = 0;
                foreach (SqlParameter parms in cmd.Parameters)
                {
                    string val = String.Empty;
                    if (parms.DbType.Equals(DbType.String) || parms.DbType.Equals(DbType.DateTime))
                        val = "'" + Convert.ToString(parms.Value).Replace(@"\", @"\\").Replace("'", @"\'") + "'";
                    if (parms.DbType.Equals(DbType.Int16) || parms.DbType.Equals(DbType.Int32) || parms.DbType.Equals(DbType.Int64) || parms.DbType.Equals(DbType.Decimal) || parms.DbType.Equals(DbType.Double))
                        val = Convert.ToString(parms.Value);

                    commandTxt.AppendLine();
                    commandTxt.Append("SET " + paramlst[rownr].ToString() + " = " + val.ToString() + ";");
                    rownr += 1;
                }
                if (cmd.Parameters.Count > 0) commandTxt.AppendLine();
                commandTxt.Append(PrettySql(cmd.CommandText));

                return commandTxt.ToString();
            }
            catch (Exception exc1)
            {
                Debug.WriteLine(exc1.Message);
                return cmd.CommandText;
            }
        }

        public static TransactionContext TxBegin(MethodBase parmMB, string parmDBMSConnect, System.Data.IsolationLevel parmIsolationLevel = System.Data.IsolationLevel.ReadCommitted)
        {
            var txCntxt = new TransactionContext();
            txCntxt.MthdBs = parmMB;
            txCntxt.Success = true;
            txCntxt.sw = new Stopwatch();
            txCntxt.sw.Start();
            txCntxt.sbSqlDeets = new StringBuilder();
            txCntxt.sbSqlDeets.AppendLine("-- Tx Start");

            var conn = ConnectionOpen(parmMB, parmDBMSConnect);
            txCntxt.Cmd = new SqlCommand();
            txCntxt.Cmd.Connection = conn;
            txCntxt.Cmd.Transaction = conn.BeginTransaction(parmIsolationLevel);
            Debug.WriteLine($"Tx Begin Transaction Isolation Level=${txCntxt.Cmd.Transaction.IsolationLevel}");
            return txCntxt;
        }

        public static bool TxEnd(TransactionContext parmTxContext, Microsoft.AspNetCore.Http.HttpContext parmHTTPContext)
        {
            var mb = parmTxContext.MthdBs;
            parmTxContext.sbSqlDeets.AppendLine("-- Tx End ");
            // Debug.WriteLine(parmTxContext.sbSqlDeets.ToString());
            if (parmTxContext.Success == false)
            {
                Debug.WriteLine($"Tx Rollback, FAIL :( in {parmTxContext.sw.ElapsedMilliseconds}ms");
                parmTxContext.Cmd.Transaction.Rollback();
            }
            else
            {
                Debug.WriteLine($"TX Commit, SUCCESS :) in {parmTxContext.sw.ElapsedMilliseconds}ms");
                parmTxContext.Cmd.Transaction.Commit();
            }
            parmTxContext.sw.Stop();
            parmTxContext.Cmd.Connection?.Dispose();
            parmTxContext.Cmd?.Dispose();
            int slowQueryThreshold = Convert.ToInt32(Configuration["SlowQueryThreshold"]);
            if (parmTxContext.sw.ElapsedMilliseconds > slowQueryThreshold)
            {
                Debug.WriteLine($"Transaction above is a Slow Query / Transaction >{slowQueryThreshold}ms");
            }
            return parmTxContext.Success;
        }
    }
}