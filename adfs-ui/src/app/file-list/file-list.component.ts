import { Component, OnInit, ViewChild } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { saveAs } from 'file-saver';
import { MatDialog } from '@angular/material';

@Component({
  selector: 'app-file-list',
  templateUrl: './file-list.component.html',
  styleUrls: ['./file-list.component.scss']
})
export class FileListComponent implements OnInit {

  files: FileEntry[];
  displayedColumns: string[] = ['name', 'actions'];

  @ViewChild('uploadField', {static:false}) uploadField;
  
  constructor(private http: HttpClient) { }



  ngOnInit() {
    this.loadFiles();
  }

  download(file: FileEntry) {
    this.http.get(environment.serviceUrl + "/files/" + file.name, { observe: 'response', responseType: 'blob' }).subscribe(
      (response: any ) => {

        saveAs(response.body, file.name);
      },
      error => {
        alert("not found");
      }
    );
  }
  delete(file: FileEntry) {
    this.http.delete(environment.serviceUrl + "/files/" + file.name).subscribe(
      (response: any ) => {
        this.loadFiles();
      },
      error => {
        alert(JSON.stringify(error));
      }
    );
  }

  addFiles() {
    this.uploadField.nativeElement.click();
  }
  
  onFilesAdded(files: File[]) {
    
    Array.from(files).forEach(file => {
      let headers = new HttpHeaders();
      headers.set('Content-Type', null);
      headers.set('Accept', "multipart/form-data");
      
      const formData: FormData = new FormData();
      formData.append('file', file, file.name);
      formData.append('key', file.name);


      this.http.post(environment.serviceUrl + "/files", formData, { headers, responseType: 'text' }).subscribe(
        data => {
          this.loadFiles();
        } , error => {
          alert(JSON.stringify(error));
        },);
    });

    
  }

  
  private loadFiles(){
    this.http.get<FileEntry[]>(environment.serviceUrl + "/files").subscribe(data => {
      this.files = data;
    });
  }

}

export interface FileEntry {
  name: string;
  date: Date;
}

