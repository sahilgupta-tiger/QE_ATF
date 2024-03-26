import { Card, CardContent, Divider, Grid, IconButton} from '@mui/material';
import { useState } from 'react';
import "../Extraction/execution.css";
import "../Overview/overview.css";
import "./connection.css";
import { ConnectionData } from '../Extraction/Data';
import CreateDataGrid from '../../Common/DataGrid';
import SelectButton from '../../Common/SelectButton';
import { Link, useNavigate } from 'react-router-dom';
import MUIModal from '../../Common/MuiModal';
import { useForm } from 'react-hook-form';
import FormProvider from '../../Common/FormProvider';

export default function ConnectionString() {
    const navigate = useNavigate();
    const [age, setAge] = useState('');
    const [open, setOpen] = useState(false);
    const [delet, setDelete] = useState(false);
    const [row, setRow] = useState("");
    const [editData, setEditData] = useState("");
    const methods = useForm();
    const { handleSubmit, reset } = methods;

    const onSubmit = (data) => {

    }

    const handleChange = (event) => {
        setAge(event.target.value);
    };

    const columns = [
        {
            field: "connection",
            sortable: false,
            headerClassName: 'super-app-theme--header',
            width: 500,
            headerAlign: "left",
            headerName: "Connections",
            renderCell: (params) => {
                const { row } = params;
                return (
                    <Grid container rowSpacing={2}>
                        <Grid item xs={12} sx={{ color: '#0CA8F2' }}>
                            <Link to={`/connection_details/${row.connection}`} className='no-underline'>{row?.connection}</Link>
                        </Grid>
                    </Grid>
                );
            },
        },
        {
            field: 'createDate',
            sortable: false,
            headerClassName: 'super-app-theme--header',
            width: 400,
            headerAlign: "left",
            headerName: 'Create Date'
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
                    <Grid container rowSpacing={2}>
                        <Grid item xs={12} sx={{ display: "flex" }}>
                            <IconButton sx={{ color: "#F7901D" }} onClick={() => {
                                setOpen(true)
                                setEditData(row)
                            }}><img src='/img/Group_170.svg' className='icon-btn' /></IconButton>
                            <Divider orientation="vertical" variant="middle" flexItem />
                            <IconButton sx={{ color: "#F7901D" }} onClick={() => {
                                setDelete(true)
                                setRow(row)
                            }}><img src='/img/Group_171.svg' className='icon-btn' /></IconButton>
                        </Grid>
                    </Grid>
                );
            },
        },

    ]

    const handleClose = () => {
        setOpen(false)
    }
    const handleCloseClose = () => {
        setDelete(false)
    }

    return (
        <div className='main-container-layout'>
            <MUIModal open={open} handleClose={handleClose} children={<>
                <Grid container spacing={2}>
                    <Grid item xs={6}>
                        <div className='first-container'>
                            <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                <Grid container spacing={2} alignItems="center">
                                    <Grid item xs={4} >
                                        Connection
                                    </Grid>
                                    <Grid item xs={8} >
                                        <input type='text' className='inputbox' value={editData.connection} />
                                    </Grid>
                                    <Grid item xs={4} >
                                        Create Date
                                    </Grid>
                                    <Grid item xs={8} >
                                        <input type='text' className='inputbox' value={editData.createDate} />
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
                title={`Are you sure you want to delete ${row.connection} ?`} action={
                    <>
                        <button className='confirm'>Yes</button>
                        <button className='cancel' onClick={handleCloseClose}>No</button></>
                } />
            <div className="heading">Connection String</div>
            <Grid container>
                <Grid item xs={12} >
                    <Card>
                        <CardContent >
                            <div className='card-container'>
                                <div className="sub-heading">Connection List</div>
                                <div>
                                    <button className='btn'><img src='/img/Icon-feather-search.png' alt='search' /></button>
                                    <button className='btn'><img src='/img/Group-7.svg' alt='search' /></button>
                                    <SelectButton label={"Age"} bgcolor={'#F4F4F4'} />
                                    <button className="create-btn" onClick={() => navigate('/new_connection')}>Create New</button>
                                </div>
                            </div>
                            <CreateDataGrid
                                columns={columns}
                                autoHeight={true}
                                rows={ConnectionData}
                                pagination={false}
                                hideFooterPagination={false}
                                rowHeight={120}
                                getRowId={(row) => row.id} />
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </div>
    )
}