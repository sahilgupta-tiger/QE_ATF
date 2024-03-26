import { Card, CardContent, Divider, Grid } from "@mui/material";
import { useNavigate, useParams } from "react-router-dom";
import "./execution.css"

export default function ExecutionFail() {
    const { id } = useParams();
    const navigate = useNavigate();

    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/execution_history')}><img src='/img/asset_19.svg' alt='back' className='image' /> Failed Result Summary</div>

            <Card>
                <CardContent >
                    <Grid container>
                        <Grid item xs={4} >
                            <div className='summary-container'>
                                <div className="first-row">
                                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                                        <div className='heading-text'>Application Name</div>
                                        <div className='response-text'>Sample</div>
                                    </div>

                                    <button className='highlight_btn'><span class="dot2" ></span>Failed</button>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Protocol Name</div>
                                    <div className='response-text'>Demo Protocol</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Failure Reason</div>
                                    <div className='response-text'>Content Mismatched</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Protocol File Path</div>
                                    <div className='response-text'>/app/test/testprotocol/testprotocol.xlsx</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Testcase Name</div>
                                    <div className='response-text'>testcase26_names_fullname_etljob</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Testcase Type</div>
                                    <div className='response-text'>Content</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Test Environment</div>
                                    <div className='response-text'>Dev</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Date</div>
                                    <div className='response-text'>23 January 2024</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Start Time</div>
                                    <div className='response-text'>14:33:34 UTC</div>
                                </div>
                                <div className='detail-cont'>
                                    <div className='heading-text'>Run Time</div>
                                    <div className='response-text'>0:01:31</div>
                                </div>


                            </div>
                        </Grid>
                        <Grid item xs={8} >
                            <div className='freeze-container'>
                            <div className='card-container ht'>
                                <div className="sub-heading">Configuration Details</div>
                            </div>
                            <Divider />
                            <Grid container spacing={3}>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Compare Type</div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : s2tcompare </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Testquery Generation Mode </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : Auto </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Testcase Type </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : content </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Source Connection Name </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : raw_oracle_database_connection</div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Source Connection Type </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : oracle </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Source Connection Value </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : raw_oracle_database_connection </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Source Format </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : table </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Source Name </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : sys.orc_patients_db</div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Source Path </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : sys.orc_patients_db/OracleDemoTiger </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Source Exclude Columns </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : DEATHDATE, SSN, DRIVERS, PASSPORT, MAIDEN, MARITAL, RACE, ETHNICITY BIRTHPLACE, ADDRESS, CITY, COUNTY, ZIP, LAT, LON, HEALTHCARE EXPENSES, H EALTHCARE_COVERAGE </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Target Connection Name </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : raw_oracle_database_connection </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Target Connection Type </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : oracle </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Target Connection Value </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : raw_oracle_database_connection </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Target Format </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : table  </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Target Name </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : sys.orc_patients_stg </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Target Path </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : sys.orc_patients_stg/OracleDemoTiger </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Target Exclude Columns </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : DEATHDATE, SSN, DRIVERS, PASSPORT, MAIDEN, MARITAL, RACE, ETHNICITY BIRTHPLACE, ADDRESS, CITY, COUNTY, ZIP, LAT, LON, HEALTHCARE EXPENSES, H EALTHCARE_COVERAGE   </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>S2T Path </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : /app/test/s2t/s2t_26_names_fullname_etljob.xlsx </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Primary Keys</div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> : ID</div>
                                </Grid>


                            </Grid>
                            <div className='card-container adding'>
                                <div className="sub-heading">Content Summary</div>
                            </div>
                            <Divider />
                            <Grid container spacing={3}>
                                <Grid item xs={4}>
                                    <div className='heading-text'>Test Result</div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  Failed </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of rows in Source </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  1171 </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of distinct rows in Source </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  1171 </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of duplicate rows in Source </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  0 </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of rows in Target </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  1169 </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of distinct rows in Target </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  1169</div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of duplicate rows in Target </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  0 </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of matched rows </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  1169 </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of mismatched rows </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  0 </div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of rows in Source but not in Target </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  2</div>
                                </Grid>
                                <Grid item xs={4}>
                                    <div className='heading-text'>No of rows in Target but not in Source </div>
                                </Grid>
                                <Grid item xs={8}>
                                    <div className='response-text2'> :  0 </div>
                                </Grid>
                               
                            </Grid>
                            <div className='card-container adding'>
                                <div className="sub-heading">SQL Queries</div>
                            </div>
                            <Divider />
                            </div>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

        </div>
    )
}