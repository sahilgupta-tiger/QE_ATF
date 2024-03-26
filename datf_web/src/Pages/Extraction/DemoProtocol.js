import { Card, CardContent, Grid } from "@mui/material";
import { useNavigate } from "react-router-dom";
import "./execution.css"
import CreateDataGrid from "../../Common/DataGrid";
import { DummyData } from "./Data";
import { Legend, BarChart, CartesianGrid, YAxis, XAxis, Tooltip, Pie ,Cell, PieChart} from 'recharts';
import { Bar } from "react-chartjs-2";
// import { PieChart } from "../../Common/PieChart";

export default function DemoProtocol() {
    const navigate = useNavigate();

    const columns = [
        {
            field: 'test',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Test Case Name',
            width: 220,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.test}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'rowsource',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Rows in Source',
            width: 120,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.rowsource}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'rowtarget',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Rows in Target',
            width: 110,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.rowtarget}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'rowmatched',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Rows Matched',
            width: 110,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.rowmatched}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'rowmismatched',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Rows Mismatched',
            width: 130,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.rowmismatched}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'runtime',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Run Time',
            width: 110,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.runtime}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'result',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Result',
            width: 90,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                <span class="dot" style={{ backgroundColor: row?.result === 'Passed' ? '#2AD170' : '#EF5350' }}></span> {row?.result}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'reason',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Reason',
            width: 150,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            {row?.result === 'Passed' ? <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left', color: '#707070' }}>
                                {row?.reason}
                            </Grid> : <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left', color: '#F7901D' }}>
                                {row?.reason}
                            </Grid>}

                        </Grid>
                    </>
                )
            },
        }
    ];

    const piedata = [
        { name: 'Green', value: 400 },
        { name: 'Red', value: 100 },
        // { name: 'Yellow', value: 100 },
    ];

    const COLORS = ['#2AD170', '#EF5350'];

    const bardata = [
        { name: 'A', value: 100 },
        { name: 'B', value: 200 },
        { name: 'C', value: 300 },
        { name: 'D', value: 400 },
      ];

    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/execution_history')}><img src='/img/asset_19.svg' alt='back' className='image' /> Connection String Details</div>
            <Card>
                <CardContent>
                    <Grid container spacing={2}>
                        <Grid item xs={3}>
                            <div className='bgcolor'>
                                <div >
                                    <div className='heading-text'>Application Name</div>
                                    <div className='response-text'>Sample</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Protocol Name</div>
                                    <div className='response-text'>Demo Protocol</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Protocol Version</div>
                                    <div className='response-text'>Dev</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Protocol File Path</div>
                                    <div className='response-text'>/app/test/testprotocol/testprotocol.xlsx</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Testcase Type</div>
                                    <div className='response-text'>Content</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Date</div>
                                    <div className='response-text'>23 January 2024</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Start Time</div>
                                    <div className='response-text'>14:33:34 UTC</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Run Time</div>
                                    <div className='response-text'>0:01:31</div>
                                </div>
                                <div className='detail-cont2'>
                                    <div className='heading-text'>Total No of Test Cases</div>
                                    <div className='response-text'>200</div>
                                </div>

                            </div>

                        </Grid>
                        <Grid item xs={9}>
                            <div className='details'>
                                <Grid container spacing={2}>
                                    <Grid item xs={3}>
                                        <div className='chart'>
                                            <PieChart width={200} height={200}>
                                                <Pie
                                                    data={piedata}
                                                    dataKey="value"
                                                    cx={80}
                                                    cy={80}
                                                    outerRadius={50}
                                                    fill="#8884d8"
                                                    label
                                                >
                                                    {piedata.map((entry, index) => (
                                                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                                    ))}
                                                </Pie>
                                                
                                                <Legend />
                                            </PieChart>
                                            {/* <PieChart height={100} innerRadius={50} outerRadius={120} data={piedata} legend={false} tooltip={false} /> */}
                                        </div>
                                    </Grid>
                                    <Grid item xs={9}>
                                        <div className='chart'>
                                            <BarChart width={500} height={200} data={bardata}>
                                                <CartesianGrid strokeDasharray="3 3" />
                                                <XAxis dataKey="name" />
                                                <YAxis dataKey="value" />
                                                <Tooltip />
                                                <Legend />
                                                <Bar dataKey="value"  
                                                maxBarSize={40}
                                                animationEasing="ease-in-out"
                                                fill={"#F7901D"}/>
                                                {/* <Bar dataKey="uv" fill="#82ca9d" /> */}
                                            </BarChart>
                                        </div>
                                    </Grid>
                                    <Grid item xs={12}>
                                        <div className="table">
                                            <CreateDataGrid
                                                columns={columns}
                                                autoHeight={true}
                                                rows={DummyData}
                                                pagination={false}
                                                hideFooterPagination={false}
                                                rowHeight={120}
                                                getRowId={(row) => row.id} />
                                        </div>
                                    </Grid>
                                </Grid>
                            </div>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>
        </div>
    )
}