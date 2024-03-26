import {Card, CardContent, Divider, Grid, IconButton, Tooltip, gridClasses } from '@mui/material';
import '../Overview/overview.css';
import "../Extraction/execution.css";
import "./testcase.css";
import MuiTabs from '../../Common/MuiTabs';
import { useEffect, useState } from 'react';
import CreateDataGrid from '../../Common/DataGrid';
import SelectButton from '../../Common/SelectButton';
import Toggle2 from './Toggle2';
import { DummyData, SecondTable } from '../Extraction/Data';
import { Link, useNavigate } from 'react-router-dom';
import MUIModal from '../../Common/MuiModal';
import { useForm } from 'react-hook-form';
import FormProvider from '../../Common/FormProvider';
import { DataGrid } from '@mui/x-data-grid';
import RHFTextField from '../../Common/RHFTextField';


export default function TestCase() {
    const [active, setActive] = useState(0);
    const navigate = useNavigate();
    const [open, setOpen] = useState(false);
    const [delet, setDelete] = useState(false);
    const [row, setRow] = useState("");
    const [editData, setEditData] = useState("");
    const [open2, setOpen2] = useState(false);
    const [open3, setOpen3] = useState(false);
    const [open4, setOpen4] = useState(false);
    const [delet2, setDelete2] = useState(false);
    const [row2, setRow2] = useState("");
    const [editData2, setEditData2] = useState("");
    const [selectedRows, setSelectedRows] = useState([]);
    const [checked, setChecked] = useState(false);
    const [selectedOption, setSelectedOption] = useState(0);
    const [optionSelect,setOptionSelect] = useState("")

    const handleChangeSelect = (e) => {
        setSelectedOption(e.target.value);
    };

    useEffect(() => {
        if (selectedOption === "1") {
            setOpen3(true)
        }
        else if (selectedOption === "2") {
            setOpen4(true)
        }
    }, [selectedOption])

    const methods = useForm();
    const { handleSubmit, reset } = methods;

    const onSubmit = (data) => {

    }

    useEffect(() => {
        if (selectedRows.length > 0) {
            setChecked(true)
        }
        else {
            setChecked(false)
        }
    }, [selectedRows])

    const columns = [
        {
            field: 'test',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Test Case Name',
            width: 500,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left', color: '#0CA8F2' }}>
                                <Link to={`/edit_test_case/${row.test}`} className='no-underline'> {row?.test}</Link>
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'createDate',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Created On',
            width: 400,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.createDate}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'date',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: '',
            width: 100,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                <IconButton sx={{ color: "#F7901D" }} onClick={() => {
                                    setOpen2(true)
                                    setEditData2(row)
                                }}><img src='/img/Group_170.svg' alt='icon' className='icon-btn' /></IconButton>
                                <Divider orientation="vertical" variant="middle" flexItem />
                                <IconButton sx={{ color: "#F7901D" }} onClick={() => {
                                    setDelete2(true)
                                    setRow2(row)
                                }}><img src='/img/Group_171.svg' alt='icon' className='icon-btn' /></IconButton>
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },

    ];

    const columns2 = [
        {
            field: "name",
            sortable: false,
            headerClassName: 'super-app-theme--header',
            width: 250,
            headerAlign: "left",
            headerName: "Protocol Name",
            renderCell: (params) => {
                const { row } = params;
                return (
                    <Grid container rowSpacing={2}>
                        <Grid item xs={12} sx={{ color: '#0CA8F2', display: 'flex' }}>
                            <input type="radio" id={row.id} value={row.id} className='radio-btn' onChange={(e)=> setOptionSelect(e.target.value)} 
                            checked={optionSelect == row.id}/>
                           
                            {/* <div style={{textDecoration: 'none'}}> */}
                            <Link to='/db_table_demo' className='no-underline'>{row?.name}</Link>
                            {/* </div> */}
                        </Grid>
                    </Grid>
                );
            },
        },
        {
            field: 'connection',
            sortable: false,
            headerClassName: 'super-app-theme--header',
            width: 125,
            headerAlign: "left",
            headerName: 'Connection'
        },
        {
            field: "filepath",
            width: 180,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "File Path",
        },
        {
            field: "app_name",
            width: 180,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "App Name",
        },
        {
            field: "environment",
            width: 180,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "Environment",
        },
        {
            field: "date",
            width: 140,
            sortable: false,
            headerClassName: 'super-app-theme--header',
            headerAlign: "left",
            headerName: "",
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                <IconButton sx={{ color: "#F7901D" }} onClick={() => {
                                    setOpen(true)
                                    setEditData(row)
                                }}><img src='/img/Group_170.svg' alt='icon' className='icon-btn' /></IconButton>
                                <Divider orientation="vertical" variant="middle" flexItem />
                                <IconButton sx={{ color: "#F7901D" }} onClick={() => {
                                    setDelete(true)
                                    setRow(row)
                                }}><img src='/img/Group_171.svg' alt='icon' className='icon-btn' /></IconButton>
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
    ]

    const handleClose = () => {
        setOpen(false)
        setOpen2(false)
        setOpen4(false)
        setOpen3(false)
        setSelectedOption(0)
    }
    const handleCloseClose = () => {
        setDelete(false)
        setDelete2(false)
    }

    return (
        <div className='main-container-layout'>
            <MUIModal open={open} handleClose={handleClose} children={<>
                <Grid container spacing={2}>
                    <Grid item xs={6}>
                        <div className='first-container'>
                            <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                <Grid container spacing={2} alignItems="center">
                                    <Grid item xs={6} >
                                        Protocol Name
                                    </Grid>
                                    <Grid item xs={6} >
                                        <input type='text' className='inputbox' value={editData.name} />
                                    </Grid>
                                    <Grid item xs={6} >
                                        Connection
                                    </Grid>
                                    <Grid item xs={6} >
                                        <input type='text' className='inputbox' value={editData.connection} />
                                    </Grid>
                                    <Grid item xs={6} >
                                        File Path
                                    </Grid>
                                    <Grid item xs={6} >
                                        <input type='text' className='inputbox' value={editData.filepath} />
                                    </Grid>
                                    <Grid item xs={6} >
                                        App Name
                                    </Grid>
                                    <Grid item xs={6} >
                                        <input type='text' className='inputbox' value={editData.app_name} />
                                    </Grid>
                                    <Grid item xs={6} >
                                        Environment
                                    </Grid>
                                    <Grid item xs={6} >
                                        <input type='text' className='inputbox' value={editData.environment} />
                                    </Grid>
                                    <Grid item xs={12} >
                                        <button className='add-btn' type='submit'>Save</button>
                                    </Grid>
                                </Grid>
                            </FormProvider>
                        </div>

                    </Grid>

                </Grid>
            </>}
                title={`Edit Details`} />
            <MUIModal open={delet2} handleClose={handleCloseClose} hideClose={true}
                title={`Are you sure you want to delete ${row2.test} ?`} action={<>
                    <button className='confirm'>Yes</button>
                    <button className='cancel' onClick={handleCloseClose}>No</button></>} />

            <MUIModal open={open2} handleClose={handleClose} children={<>
                <Grid container spacing={2}>
                    <Grid item xs={6}>
                        <div className='first-container'>
                            <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                <Grid container spacing={2} alignItems="center">
                                    <Grid item xs={6} >
                                        Test Case Name
                                    </Grid>
                                    <Grid item xs={6} >
                                        <input type='text' className='inputbox' value={editData2.test} />
                                    </Grid>
                                    <Grid item xs={6} >
                                        Create Date
                                    </Grid>
                                    <Grid item xs={6} >
                                        <input type='text' className='inputbox' value={editData2.createDate} />
                                    </Grid>

                                    <Grid item xs={12} >
                                        <button className='add-btn' type='submit'>Save</button>
                                    </Grid>
                                </Grid>
                            </FormProvider>
                        </div>

                    </Grid>

                </Grid>
            </>}
                title={`Edit Details`} />
            <MUIModal open={delet} handleClose={handleCloseClose} hideClose={true}
                title={`Are you sure you want to delete ${row.name} ?`} action={
                    <>
                        <button className='confirm'>Yes</button>
                        <button className='cancel' onClick={handleCloseClose}>No</button></>
                } />
            <div className='sub-container'>
                <div className="heading">Testcase Setup</div>
                <div>
                    <button className="sql-btn" onClick={() => navigate('/sql_setup')}><img src='/img/Subtraction_1.svg' alt='setting' /> SQL Setup</button>
                    <button className="mapping-btn" onClick={() => navigate('/mapping_config')} ><img src='/img/Subtraction_1.svg' alt='setting' /> Mapping Config</button>
                    <button className="execute-btn"><img src='/img/Polygon_3.svg' alt='setting' /> Execute</button>
                </div>

            </div>

            <Grid container>
                <Grid item xs={12} >
                    <Card>
                        <CardContent >
                            <div className='card-container'>
                                <MuiTabs tabs={Toggle2} active={active} setActive={setActive} />
                                <div>
                                    {active === 2 &&
                                        <button className='btn'><img src='/img/Icon-feather-search.png' alt='search' /></button>}
                                    {active === 0 &&
                                        <>
                                            {checked ? <Tooltip title='All Delete'><button className='btn'><img src='/img/remove-folder-svgrepo-com.svg' alt='search' className='all-delete' /></button></Tooltip> :
                                                <button className='btn disabled-button'><img src='/img/remove-folder-svgrepo-com.svg' alt='search' className='all-delete' /></button>}</>
                                    }

                                    <button className='btn'><img src='/img/Group-7.svg' alt='search' /></button>

                                    <SelectButton label={"Age"} bgcolor={'#F4F4F4'} />
                                    {active === 0 &&
                                        <>
                                            {checked ?
                                                // <button className="create-btn" onClick={() => {}}>Protocol</button>
                                                <>
                                                    <select className="select-btn" value={selectedOption} onChange={handleChangeSelect}>
                                                        <option value="0" disabled hidden>{selectedOption ? selectedOption : "Protocol"}</option>
                                                        <option value="1">Create a New Protocol</option>
                                                        <option value="2">Add to Existing Protocol</option>
                                                    </select>
                                                </>

                                                :

                                                <button className="create-btn disabled-button">Protocol</button>
                                            }

                                            <button className="create-btn" onClick={() => navigate('/new_test_case')}>Create New</button>
                                        </>}
                                </div>
                            </div>
                            {active === 0 &&
                                <DataGrid
                                    columns={columns}
                                    autoHeight={true}
                                    rows={DummyData}
                                    checkboxSelection
                                    onRowSelectionModelChange={(ids) => {
                                        const selectedIDs = new Set(ids);
                                        const selectedRows = DummyData?.filter((row) =>
                                            selectedIDs.has(row.id),
                                        );

                                        setSelectedRows(selectedRows);
                                    }}
                                    selectedRows={selectedRows}
                                    {...DummyData}
                                    sx={{
                                        border: 0,
                                        // font: 'normal normal normal 14px/50px Articulat CF',
                                        [`& .${gridClasses.row}`]: {
                                            bgcolor: (theme) =>
                                                theme.palette.mode === 'light' ? "#FFF" : '#707070',
                                            font: 'normal normal normal 14px/50px Articulat CF',
                                            fontWeight: 400,
                                            color: '#707070',
                                            height: '30px',
                                        },
                                        '& .super-app-theme--header': {
                                            backgroundColor: '#F4F4F4',
                                            // borderRadius: '5px',
                                            height: '20px',
                                            font: 'normal normal bold 14px/50px Articulat CF',
                                            fontWeight: 700,

                                        },
                                        // '.MuiTablePagination-displayedRows': {
                                        //   display: (hideCountNumber) ? 'none' : 'false', // 👈 to hide huge pagination number
                                        // },
                                    }}
                                />
                            }
                            {active === 2 &&
                                <CreateDataGrid
                                    columns={columns2}
                                    autoHeight={true}
                                    rows={SecondTable}
                                    pagination={false}
                                    hideFooterPagination={false}
                                    rowHeight={120}
                                    getRowId={(row) => row.id} />
                            }

                            {/* Create New Protocol */}
                            <MUIModal open={open3} handleClose={handleClose} hideClose={true} children={<>
                                <Grid container spacing={2}>
                                    <Grid item xs={6}>
                                        <div className='testCases'>
                                            <div className='selected-testcase'>
                                                {selectedRows.length + " Test Cases Selected"}
                                                <Divider />
                                            </div>

                                            <ul className='list-item'>
                                                {selectedRows.map((item) => (
                                                    <li>{item.test}</li>
                                                ))}
                                            </ul>
                                        </div>
                                    </Grid>
                                    <Grid item xs={6}>
                                        <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Name</div>
                                                    <RHFTextField name='protocol_name' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Connection</div>
                                                    <RHFTextField name='protocol_connection' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Result Path</div>
                                                    <RHFTextField name='protocol_path' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Application Name</div>
                                                    <RHFTextField name='application_name' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Run Environment</div>
                                                    <RHFTextField name='run_enivronment' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Version</div>
                                                    <RHFTextField name='protocol_version' className='form-inputbox' />
                                                </div>
                                            </div>
                                        </FormProvider>
                                    </Grid>

                                </Grid>
                            </>} title={`Create New Protocol`} action={<>
                                <button className='create-btn' onClick={handleClose}>Cancel</button>
                                <button type='onSubmit' className='save-btn'>Create</button></>} />

                            {/* Edit Exitising Protocol */}
                            <MUIModal open={open4} handleClose={handleClose} hideClose={true} children={<>
                                <Grid container spacing={2}>
                                    <Grid item xs={6}>
                                        <div className='testCases'>
                                            <div className='selected-testcase'>
                                                {selectedRows.length + " Test Cases Selected"}
                                                <Divider />
                                            </div>

                                            <ul className='list-item'>
                                                {selectedRows.map((item) => (
                                                    <li>{item.test}</li>
                                                ))}
                                            </ul>
                                        </div>
                                    </Grid>
                                    <Grid item xs={6}>
                                        <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Name</div>
                                                    <RHFTextField name='protocol_name' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Connection</div>
                                                    <RHFTextField name='protocol_connection' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Result Path</div>
                                                    <RHFTextField name='protocol_path' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Application Name</div>
                                                    <RHFTextField name='application_name' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Run Environment</div>
                                                    <RHFTextField name='run_enivronment' className='form-inputbox' />
                                                </div>
                                            </div>
                                            <div className='form-data'>
                                                <div className='form-box'>
                                                    <div className='form-text'>Protocol Version</div>
                                                    <RHFTextField name='protocol_version' className='form-inputbox' />
                                                </div>
                                            </div>
                                        </FormProvider>
                                    </Grid>

                                </Grid>
                            </>} title={`Create New Protocol`} action={<>
                                <button className='create-btn' onClick={handleClose}>Cancel</button>
                                <button type='onSubmit' className='save-btn'>Add</button></>} />
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </div >
    )
}