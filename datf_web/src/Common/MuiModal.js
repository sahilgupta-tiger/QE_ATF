import * as React from 'react';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import { Button, DialogActions } from '@mui/material';

export default function MUIModal(props) {
    const { open, handleClose, children, title,child ,action, hideClose,} = props;
    
    return (
        <Dialog
            open={open}
            onClose={handleClose}
            maxWidth="sm"
            fullWidth={true}
            scroll="body"
        >
            <DialogTitle>
                {title}
                {child}
            </DialogTitle>
            <DialogContent component="div">
                {children}
            </DialogContent>
            <DialogActions>
                {action}
                {!hideClose && <Button onClick={handleClose}>Close</Button>}
            </DialogActions>
        </Dialog>
    );
}