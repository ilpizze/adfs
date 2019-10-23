import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { FileListComponent } from './file-list/file-list.component';
import { SlaveListComponent } from './slave-list/slave-list.component';

const routes: Routes = [
  { 
    path: 'files',
    component: FileListComponent
  },
  { 
    path: 'slaves',
    component: SlaveListComponent
  },
  { path: '',
    redirectTo: '/files',
    pathMatch: 'full'
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
