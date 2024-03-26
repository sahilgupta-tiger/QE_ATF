import "./topbar.css";
import React from 'react';
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Toolbar from '@mui/material/Toolbar';
import { Avatar, Button} from '@mui/material';
import PopupState, { bindTrigger } from 'material-ui-popup-state';


// const menuPaperProps = {
//     sx: {
//         borderRadius: 1,
//         '& li': {
//             px: 2,
//             py: 2
//         },
//         p: 0,
//         overflow: 'visible',
//         '& .MuiAvatar-root': {
//             width: 32,
//             height: 32,
//             ml: -0.5,
//             mr: 1,
//         },
//         '&:before': {
//             content: '""',
//             display: 'block',
//             position: 'absolute',
//             top: 0,
//             right: 14,
//             width: 10,
//             height: 10,
//             transform: 'translateY(-50%) rotate(45deg)',
//             zIndex: 0,
//         },
//     },
// }

export default function TopBar() {

    return (
        <>
            <AppBar position="fixed" sx={{
                background: '#fff',
                boxShadow: 0,
                borderBottom: "1px solid rgba(145, 158, 171, 0.24)",
                width: {
                    md: 'calc(100% - 240px)'
                },
            
            }}>
                <Toolbar>

                    <div class="search-container">
                        <img src='/img/Icon-feather-search.svg' alt='search'/>
                        <input type="text" placeholder="Search" />

                    </div>
                    <Box sx={{ flexGrow: 1 }} ></Box>
                    <PopupState variant="popover" popupId="profile-menu">
                        {(popupState) => (
                            <React.Fragment>
                                <Button
                                    {...bindTrigger(popupState)}
                                    size="large"
                                    endIcon={<img src="/img/Path-1.svg" alt='avatar' />}
                                >
                                </Button>

                                <Button
                                    {...bindTrigger(popupState)}
                                    size="large"
                                    sx={{
                                        textTransform: 'capitalize',
                                        // color: '#212b36c2',
                                    }}
                                    endIcon={<Avatar sx={{
                                        // backgroundColor: orange[300],
                                        borderRadius: '50%',
                                        height: 35,
                                        width: 35,
                                        color: 'black'
                                    }}>
                                        <img src="/img/icon-5359553_1280.webp" alt='avatar' style={{height:'35px',width:'35px'}} />
                                    </Avatar>}
                                >
                                </Button>
                            </React.Fragment>
                        )}
                    </PopupState>
                </Toolbar>
            </AppBar>
        </>
    )
}