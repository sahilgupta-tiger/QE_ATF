import { Card, CardContent, Divider, Grid, IconButton} from '@mui/material';
import { useState } from 'react';
import "../Extraction/execution.css";
import "../Overview/overview.css";
import "./sql.css";
import "../ConnectionString/connection.css";
import { ConnectionData } from '../Extraction/Data';
import CreateDataGrid from '../../Common/DataGrid';
import SelectButton from '../../Common/SelectButton';
import { useNavigate } from 'react-router-dom';
import MUIModal from '../../Common/MuiModal';

export default function SQLSetup() {
    const navigate = useNavigate();
    const [open, setOpen] = useState(false);
    const [delet, setDelet] = useState(false);
    const [row,setRow] = useState("")
    const [row2,setRow2] = useState("")

    const handleClose = () => {
        setOpen(false);
        setDelet(false);
    }

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
                            {row?.connection}
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
            headerName: 'Created On'
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
                                setOpen(true);
                                setRow(row)
                            }}><img src='/img/Group_170.svg' alt='icon' className='icon-btn' /></IconButton>
                            <Divider orientation="vertical" variant="middle" flexItem />
                            <IconButton sx={{ color: "#F7901D" }} onClick={() => {
                                setDelet(true);
                                setRow2(row)
                            }}><img src='/img/Group_171.svg' alt='icon' className='icon-btn' /></IconButton>
                        </Grid>
                    </Grid>
                );
            },
        },

    ]
    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/test_case_setup')}><img src='/img/asset_19.svg' alt='back' className='image' /> SQL Setup</div>
            <Grid container>
                <Grid item xs={12} >
                    <Card>
                        <CardContent >
                            <div className='card-container'>
                                <div className="sub-heading">SQL List</div>
                                <div>
                                    <button className='btn'><img src='/img/Icon-feather-search.png' alt='search' /></button>
                                    <button className='btn'><img src='/img/Group-7.svg' alt='search' /></button>

                                    <SelectButton label={"Age"} bgcolor={'#F4F4F4'} />
                                    <button className="create-btn" onClick={() => navigate("/create_sql")}>Create New</button>
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

                            <MUIModal open={delet} handleClose={handleClose} hideClose={true}
                                title={`Are you sure you want to delete ${row2.connection} ?`} action={<>
                                    <button className='confirm'>Yes</button>
                                    <button className='cancel' onClick={handleClose}>No</button></>} />

                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </div>
    )
}