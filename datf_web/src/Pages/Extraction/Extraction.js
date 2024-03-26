import { Card, CardContent, Grid, } from '@mui/material';
import '../Overview/overview.css';
import "./execution.css";
import MuiTabs from '../../Common/MuiTabs';
import Toggle from './Toggle';
import { useState } from 'react';
import CreateDataGrid from '../../Common/DataGrid';
import { DummyData, SecondTable } from './Data';
import SelectButton from '../../Common/SelectButton';
import { Link } from 'react-router-dom';


export default function Extraction() {
    const [active, setActive] = useState(0)
    const [age, setAge] = useState('');

    const handleChange = (event) => {
        setAge(event.target.value);
    };

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
                            </Grid> : <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left', color:'#F7901D'}}>
                               <Link to={`/execution_fail/${row.id}`} className='no-underline2'>{row?.reason}</Link> 
                            </Grid>}
                            
                        </Grid>
                    </>
                )
            },
        }
    ];

    const columns2 = [
        {
            field: "name",
            sortable: false,
            headerClassName: 'super-app-theme--header',
            width: 220,
            headerAlign: "left",
            headerName: "Protocol Name",
              renderCell: (params) => {
                const { row } = params;
                return (
                    <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{color: '#0CA8F2' }}>
                               <Link to={`/demo_protocol/${row.id}`} className='no-underline'>{row?.name}</Link> 
                            </Grid>
                        </Grid>
                );
              },
        },
        {
            field: 'version',
            sortable: false,
            headerClassName: 'super-app-theme--header',
            width: 125,
            headerAlign: "left",
            headerName: 'Version'
        },
        {
            field: "environment",
            width: 140,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "Environment",
        },
        {
            field: "test_cases",
            width: 140,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "No of Test Cases",
        },
        {
            field: "runtime",
            width: 140,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "Run Time",
        },
        {
            field: "passed_test",
            width: 140,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "Passed Test Cases",
        },
        {
            field: "failed_test",
            width: 140,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "Failed Test Cases",
        },
    ]
    
    return (
        <div className='main-container-layout'>
            <div className="heading">Execution History</div>
            <Grid container>
                <Grid item xs={12} >
                    <Card>
                        <CardContent >
                            <div className='card-container'>
                                <MuiTabs tabs={Toggle} active={active} setActive={setActive} />
                                <div>
                                    <button className='btn'><img src='/img/Icon-feather-search.png' alt='search' /></button>
                                    <button className='btn'><img src='/img/Group-7.svg' alt='search' /></button>
                                    {/* <FormControl sx={{ m: 1.5, minWidth: 120, bgcolor: '#E8E8E8', borderRadius: 'none' }} size="small">
                                        <InputLabel id="demo-select-small-label">Last Year</InputLabel>
                                        <Select
                                            labelId="demo-select-small-label"
                                            id="demo-select-small"
                                            value={age}
                                            label="Age"
                                            onChange={handleChange}
                                        >
                                            <MenuItem value="">
                                                <em>None</em>
                                            </MenuItem>
                                            <MenuItem value={10}>Ten</MenuItem>
                                            <MenuItem value={20}>Twenty</MenuItem>
                                            <MenuItem value={30}>Thirty</MenuItem>
                                        </Select>
                                    </FormControl> */}
                                    <SelectButton bgcolor={'#F4F4F4'}/>
                                </div>
                            </div>
                            {active === 2 && 
                            <CreateDataGrid
                                columns={columns}
                                autoHeight={true}
                                rows={DummyData}
                                pagination={false}
                                hideFooterPagination={false}
                                rowHeight={120}
                                getRowId={(row) => row.id} />
                            }
                             {active === 0 && 
                            <CreateDataGrid
                                columns={columns2}
                                autoHeight={true}
                                rows={SecondTable}
                                pagination={false}
                                hideFooterPagination={false}
                                rowHeight={120}
                                getRowId={(row) => row.id} />
                            }
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </div>

    )
}