import { Card, CardContent, Grid, IconButton } from "@mui/material";
import CreateDataGrid from "../../Common/DataGrid";
import { DBData, DummyData } from "../Extraction/Data";
import SelectButton from "../../Common/SelectButton";
import { useNavigate } from "react-router-dom";
import '../Overview/overview.css';
import "../Extraction/execution.css";
import "./testcase.css";

export default function DBTableDemo() {
    const navigate = useNavigate();

    const columns = [
        {
            field: 'name',
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
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left', color: 'black' }}>
                                {row?.name}
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        {
            field: 'path',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'File Path',
            width: 400,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.path}
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
            width: 400,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                            <IconButton sx={{ color: "#F7901D" }}><img src='/img/Group_171.svg' className='icon-btn' /></IconButton>
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
    ]


    return (
        <div className='main-container-layout'>
            <div className='sub-container'>
                <div className="heading">Testcase Setup</div>
                <div>

                    <button className="execute-btn"><img src='/img/Polygon_3.svg' alt='setting' /> Execute</button>
                </div>
            </div>
            <Grid container>
                <Grid item xs={12} >
                    <Card>
                        <CardContent >
                            <div className='card-container'>
                                <div className="sub-heading dbData" onClick={() => navigate('/test_case_setup')}><img src='/img/asset_19.svg' alt='back' className='image' />DBTablesDemo</div>
                                <div>
                                    <button className='btn'><img src='/img/Icon-feather-search.png' alt='search' /></button>
                                    <button className='btn'><img src='/img/Group-7.svg' alt='search' /></button>
                                    <SelectButton bgcolor={'#F4F4F4'} />
                                </div>
                            </div>

                            <CreateDataGrid
                                    columns={columns}
                                    autoHeight={true}
                                    rows={DBData}
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