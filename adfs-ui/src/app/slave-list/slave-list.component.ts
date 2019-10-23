import { Component, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { Observable, Subject, timer, of } from 'rxjs';
import { switchMap, takeUntil, catchError } from 'rxjs/operators';

@Component({
  selector: 'app-slave-list',
  templateUrl: './slave-list.component.html',
  styleUrls: ['./slave-list.component.scss']
})
export class SlaveListComponent implements OnInit {

  constructor(private http: HttpClient) { }
  slaves: string[];
  killTrigger: Subject<void> = new Subject();

  ngOnInit() {
    timer(0, 5000).pipe(
      takeUntil(this.killTrigger),
      
    ).subscribe(() => {
      this.loadSlaves();
    });
    
  }
  private loadSlaves(){
    this.http.get<string[]>(environment.serviceUrl + "/slaves").subscribe(data => {
      this.slaves = data;
    });
  }

  ngOnDestroy(){
    this.killTrigger.next();
  }
}
