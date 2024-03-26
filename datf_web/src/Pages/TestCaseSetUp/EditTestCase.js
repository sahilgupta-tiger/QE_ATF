import { useNavigate, useParams } from "react-router-dom";
import "../Extraction/execution.css";
import "../Overview/overview.css";
import "../NewConnection/newconnection.css";
import "../NewTestCase/newtestCase.css";
import { Card, CardContent, Divider} from '@mui/material';
import { useForm } from "react-hook-form";
import FormProvider from "../../Common/FormProvider";
import RHFSelectField from "../../Common/RHFSelectField";
import RHFTextField from "../../Common/RHFTextField";


export default function EditTestCase() {
    const {id} = useParams();
    const navigate = useNavigate();
    const methods = useForm();
    const { handleSubmit, reset } = methods;

    const onSubmit = (data) => {

    }

    const Connection_type = ["simple", "complex"]
    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/test_case_setup')}><img src='/img/asset_19.svg' alt='back' className='image' /> {id}</div>
            {/* <Grid container>
                <Grid item xs={12} > */}
            <Card>
                <CardContent >
                    <div className='card-container'>
                        <div className="form-sub-heading">General Details</div>
                    </div>
                    <Divider />
                    <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Testcase Name</div>
                                {/* <input type='text' className='form-inputbox' /> */}
                                <RHFTextField name='testcase_name' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Compare Type</div>
                                <RHFTextField name='compare_type' className='form-inputbox' />
                            </div>

                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Test Query Genertaion Mode</div>
                                <RHFTextField name='test_query' className='form-inputbox' />
                            </div>
                        </div>
                        <div className='card-container'>
                            <div className="form-sub-heading">Source Details</div>

                        </div>
                        <Divider />

                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Source Alias Name</div>
                                <RHFTextField name='source_name' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Source Connection Name</div>
                                <RHFTextField name='source_connection' className='form-inputbox' />
                            </div>

                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Source Connection Type</div>
                                <RHFSelectField name='source_connection_type' variant='outlined' className='form-inputbox' options={Connection_type.map((val) => ({
                                    value: val,
                                    label: val,
                                }))} width="260px" />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Source File Format</div>
                                <RHFSelectField name='source_file' variant='outlined' className='form-inputbox' options={Connection_type.map((val) => ({
                                    value: val,
                                    label: val,
                                }))} width="260px" />
                            </div>
                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Source File Path</div>
                                <RHFTextField name='source_file_path' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Source File Name</div>
                                <RHFTextField name='source_file_name' className='form-inputbox' />
                            </div>
                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Source File has Header</div>
                                <RHFTextField name='source_header' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Source File Delimiter</div>
                                <RHFTextField name='source_delimiter' className='form-inputbox' />
                            </div>
                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Source Filter</div>
                                <RHFTextField name='source_filter' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Source Exclude Column List</div>
                                <RHFTextField name='source_exclude' className='form-inputbox' />
                            </div>
                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Source Query columnVisibilityModel</div>
                                <RHFTextField name='source_visibilitymodel' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Source Query SQL Path</div>
                                <RHFTextField name='source_sql' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Source Query SQL File Name</div>
                                <RHFTextField name='source_sql_file' className='form-inputbox' />
                            </div>
                        </div>

                        <div className='card-container'>
                            <div className="form-sub-heading">Target Details</div>

                        </div>
                        <Divider />

                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Target Alias Name</div>
                                <RHFTextField name='target_name' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Target Connection Name</div>
                                <RHFTextField name='target_connection' className='form-inputbox' />
                            </div>

                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Target Connection Type</div>
                                <RHFSelectField name='target_connection' variant='outlined' className='form-inputbox' options={Connection_type.map((val) => ({
                                    value: val,
                                    label: val,
                                }))} width="260px" />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Target File Format</div>
                                <RHFSelectField name='target_format' variant='outlined' className='form-inputbox' options={Connection_type.map((val) => ({
                                    value: val,
                                    label: val,
                                }))} width="260px" />
                            </div>
                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Target File Format</div>
                                <RHFSelectField name='targe_file_format' variant='outlined' className='form-inputbox' options={Connection_type.map((val) => ({
                                    value: val,
                                    label: val,
                                }))} width="260px" />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Target File Description</div>
                                <RHFTextField name='target_discription' className='form-inputbox2' />
                                {/* <input type='text' className='form-inputbox2' /> */}
                            </div>
                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Target File Path</div>
                                <RHFTextField name='target_file_path' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Target File Name</div>
                                <RHFTextField name='target_file_name' className='form-inputbox' />
                            </div>
                        </div>

                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Target Filter</div>
                                <RHFTextField name='target_filter' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Target Exclude Column List</div>
                                <RHFTextField name='target_exclude' className='form-inputbox' />
                            </div>
                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Target Query Model</div>
                                <RHFTextField name='target_query' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Target Query SQL Path</div>
                                <RHFTextField name='target_sql' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Target Query SQL File Name</div>
                                <RHFTextField name='target_sql_name' className='form-inputbox' />
                            </div>
                        </div>

                        <div className='card-container'>
                            <div className="form-sub-heading">Other Details</div>

                        </div>
                        <Divider />

                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>s2t Path</div>
                                <RHFTextField name='s2t_path' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>s2t Mapping Sheet</div>
                                <RHFTextField name='s2t_mapping' className='form-inputbox' />
                            </div>

                        </div>
                        <div className='form-data'>
                            <div className='form-box'>
                                <div className='form-text'>Primary Key</div>
                                <RHFTextField name='primary_key' className='form-inputbox' />
                            </div>
                            <div className='form-box'>
                                <div className='form-text'>Sample Limit</div>
                                <RHFTextField name='sample_limit' className='form-inputbox' />
                            </div>

                        </div>
                        <div className='divider'>
                            {/* <div className="form-sub-heading">Other Details</div> */}
                            <Divider />
                        </div>
                        <div className='butn'>
                            <button className='create-btn'>Cancel</button>
                            <button type='Submit' className='save-btn'>Save</button>
                        </div>
                    </FormProvider>

                </CardContent>
            </Card>
            {/* </Grid>
            </Grid> */}
        </div>
    )
}