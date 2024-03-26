import { Card, CardContent, Grid, IconButton} from '@mui/material';
import '../Overview/overview.css';
import "../Extraction/execution.css";
import "./config.css";
import MuiTabs from '../../Common/MuiTabs';
import { useState } from 'react';
import CreateDataGrid from '../../Common/DataGrid';
import ToggleConfig from './ToggleConfig';
import { DummyData } from '../Extraction/Data';
import FormProvider from '../../Common/FormProvider';
import { useForm } from 'react-hook-form';


export default function Configuration() {
    const [active, setActive] = useState(0);
    const methods = useForm();
    const { handleSubmit, reset } = methods;

    const onSubmit = (data) => {

    }

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
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                                {row?.test}
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
            headerName: 'Uploaded On',
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
            field: 'rowtarget',
            headerAlign: "left",
            hideSortIcons: true,
            headerClassName: 'super-app-theme--header',
            headerName: 'Rows in Target',
            width: 150,
            align: 'left',
            renderCell: (params) => {
                const { row } = params;
                return (
                    <>
                        <Grid container rowSpacing={2}>
                            <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'left' }}>
                            <IconButton sx={{color:"#F7901D"}}><img src='/img/Group_171.svg' alt='icon'className='icon-btn'/></IconButton>
                            </Grid>
                        </Grid>
                    </>
                )
            },
        },
        
    ];

    
    return (
        <div className='main-container-layout'>
            <div className="heading">Configuration</div>
            <Grid container>
                <Grid item xs={12} >
                    <Card>
                        <CardContent >
                        <div className='card-container'>
                                <MuiTabs tabs={ToggleConfig} active={active} setActive={setActive} />
                                <div>
                               {active === 2 && 
                                <button className='upload-btn'>Upload</button>}
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
                                <Grid container spacing={2}>
                                <Grid item xs={6}>
                                    <div className='first-container'>
                                        <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                            <Grid container spacing={2} alignItems="left">
                                                <Grid item xs={4} >
                                                   <span className='form-text'>App Name</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Executor Instances</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Executor Cores</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Executor Memory</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Default parallelism</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>SQL Shuffle Partitions</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Memory OffHeap Enabled</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Memory OffHeap Size</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Memory Fraction</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>Memory Storage Fraction</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>SQL Debug MaxToString Fields</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>SQL Legacy Time Parser Policy</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={4} >
                                                <span className='form-text'>SQL Auto Broadcast Join Threshold</span>
                                                </Grid>
                                                <Grid item xs={8} >
                                                   <input type='text' className='inputbox-field'/>
                                                </Grid>
                                                <Grid item xs={12} >
                                                  <button className='add-btn'> <img src='/img/Group_1389.svg' alt='add' className="add-image"/> Add More Field</button>
                                                </Grid>
                                            </Grid>
                                        </FormProvider>
                                    </div>

                                </Grid>
                                <Grid item xs={6}>
                                    <div className='second-container'>
                                        <div className='second-container-text2'>
                                            <ul className='list'>
                                                <li>
                                                .setAppName(‘s2ttester’) 
                                                </li>
                                                <li> .set(“spark.executor.instances”, “18”) </li>
                                                <li>.set(“spark.executor.cores”, “8”)</li>
                                                <li>.set(“spark.executor.memory”, “6g”) </li>
                                                <li>.set(“spark.default.parallelism”, “56”) </li>
                                                <li>.set(“spark.sql.shuffle.partitions”, “250”)</li>
                                                <li>.set(“spark.memory.offHeap.enabled”, “true”) </li>
                                                <li>.set(“spark.memory.offHeap.size”, “2g”) </li>
                                                <li>.set(“spark.memory.fraction”, “0.8”) </li>
                                                <li> .set(“spark.memory.storageFraction”, “0.6”) </li>
                                                <li>.set(“spark.sql.debug.maxToStringFields”, “300”) </li>
                                                <li>.set(“spark.sql.legacy.timeParserPolicy”, “LEGACY”)</li>
                                                <li>.set(“spark.sql.autoBroadcastJoinThreshold”, “-1”)</li>
                                            </ul>
                                        
                                        </div>
                                    </div>
                                </Grid>
                            </Grid>
                            }
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </div>
    )
}